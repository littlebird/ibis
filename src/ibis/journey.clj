(ns ibis.journey
  (:require
   [clojure.core.async :as >]
   [clj-time.core :as time]
   [taoensso.timbre :as log]
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
  [{:keys [store zookeeper zookeeper-string decoders]} course]
  (let [journey-id (java.util.UUID/randomUUID)
        topic (str "ibis-journey-" journey-id)
        consumer (kafka/make-consumer zookeeper-string (str journey-id) {})
        receive (kafka/make-receive consumer topic decoders)
        journey {:id journey-id :course course :topic topic}]
    (log/info "SUBMIT!" course)
    (zoo/create zookeeper ["ibis" "journeys" journey-id "segments"])
    (kafka/create-topic zookeeper-string topic 1)
    (store :journey (assoc journey :started (time/now)))
    (assoc journey :consumer consumer :receive receive)))

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
  (log/debug
   "LANDED" id
   "segment" segment-id
   "- paths:segments:landed"
   (str (count paths) ":" (count (get segments segment-id)) ":" (count landed)))
  (if (= (count paths) (count (get segments segment-id)))
    (do
      (zoo/delete zookeeper ["ibis" "journeys" id "segments" segment-id])
      (let [children (zoo/count-children zookeeper ["ibis" "journeys" id "segments"])]
        (and
         (zero? children)
         (= (count paths)
            (count landed)))))
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
