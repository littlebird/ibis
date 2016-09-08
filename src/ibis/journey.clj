(ns ibis.journey
  (:require
   [clojure.core.async :as >]
   [clj-time.core :as time]
   [taoensso.timbre :as log]
   [clj-kafka.consumer.zk :as kafka.consumer]
   [clj-kafka.admin :as kafka.admin]
   [ibis.zookeeper :as zoo]
   [ibis.kafka :as kafka]))

(defn paths-from-start
  [course start]
  (let [branches (get course start)]
    (if (empty? branches)
      [[start]]
      (map
       (partial cons start)
       (mapcat (partial paths-from-start course) branches)))))

(defn course->paths
  [course]
  (map vec (paths-from-start course :in)))

(defn submit!
  [{:keys [store zookeeper zookeeper-string decoders]} course janitor]
  {:pre [(not (realized? janitor))]
   :post [(realized? janitor)]}
  (let [journey-id (java.util.UUID/randomUUID)
        topic (str "ibis-journey-" journey-id)
        consumer (kafka/make-consumer zookeeper-string (str journey-id) {})
        receive (kafka/make-receive consumer topic decoders)
        journey {:id journey-id :course course :topic topic}
        tracking-path ["ibis" "journeys" journey-id "segments"]
        thunk (delay
               (let [client (kafka.admin/zk-client zookeeper-string)]
                 (try
                   (receive :close)
                   (kafka.consumer/shutdown consumer)
                   (zoo/delete zookeeper tracking-path)
                   (kafka.admin/delete-topic client topic)
                   (finally (.close client)))))
        cleanup (fn []
                  (force thunk)
                  nil)]
    (deliver janitor cleanup)
    (log/info "SUBMIT!" course)
    (zoo/create zookeeper tracking-path)
    (kafka/create-topic zookeeper-string topic 1)
    (store :journey (assoc journey :started (time/now)))
    (assoc journey
           :consumer consumer
           :receive receive)))

(defn push!
  [{:keys [transmit zookeeper]} journey segment]
  (let [segment-id (java.util.UUID/randomUUID)]
    (log/info "PUSH!" segment-id)
    (zoo/create zookeeper ["ibis" "journeys" (:id journey) "segments" segment-id])
    (transmit
     {:journey (dissoc journey :consumer :receive)
      :stage :in
      :message segment
      :traveled []
      :segment-id segment-id})))

(defn finish!
  [ibis journey]
  (push! ibis journey :land))

(defn all-landed?
  [{:keys [zookeeper] :as ibis}
   {:keys [id] :as journey}
   paths segments landed
   {:keys [segment-id] :as segment}]
  (if (= (count paths) (count (get segments segment-id)))
    (let [_ (zoo/delete zookeeper ["ibis" "journeys" id "segments" segment-id])
          children (zoo/count-children zookeeper ["ibis" "journeys" id "segments"])
          done? (and
                 (zero? children)
                 (= (count paths)
                    (count landed)))]
      (when done?
        (log/debug (:course journey) "LANDED" id "segment" segment-id)
        true))
    false))

(defn clean-up!
  [{:keys [zookeeper zookeeper-connect]} {:keys [id topic consumer]}]
  (kafka/shutdown-consumer consumer)
  ;; (kafka/delete-topic zookeeper-connect topic)
  (zoo/delete zookeeper ["ibis" "journeys" id]))

(defn pull!
  ([ibis journey reducer initial]
   (pull! ibis journey reducer initial nil))
  ([ibis {:keys [receive course] :as journey} reducer initial chan]
   (let [paths (course->paths course)]
     (loop [segment (receive)
            segments {}
            landed []]
       (let [segments (update segments (:segment-id segment) conj segment)
             landed (if (= :land (:message segment)) (conj landed segment) landed)]
         (if chan
           (>/put! chan segment))
         (if (all-landed? ibis journey paths segments landed segment)
           (let [landed-id (:segment-id (first landed))
                 output-segments (mapv :message (apply concat (vals (dissoc segments landed-id))))
                 results (reduce reducer initial output-segments)]
             (clean-up! ibis journey)
             results)
           (recur (receive) segments landed)))))))
