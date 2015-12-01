(ns ibis.journey
  (:require
   [clj-time.core :as time]
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
  [{:keys [store zookeeper zookeeper-host zookeeper-port decoders]} course]
  (let [journey-id (java.util.UUID/randomUUID)
        topic (str "ibis-journey-" journey-id)
        consumer (kafka/make-consumer zookeeper-host zookeeper-port (str journey-id) {})
        receive (kafka/make-receive consumer topic decoders)
        journey {:id journey-id :course course :topic topic}]
    (println "SUBMIT!" course)
    (zoo/create zookeeper ["ibis" "journeys" journey-id "segments"])
    (kafka/create-topic (str zookeeper-host \: zookeeper-port) topic 1)
    (store :journey (assoc journey :started (time/now)))
    (assoc journey :receive receive)))

(defn push!
  [{:keys [transmit zookeeper]} journey segment]
  (let [segment-id (java.util.UUID/randomUUID)]
    (println "PUSH!" segment-id)
    (zoo/create zookeeper ["ibis" "journeys" (:id journey) "segments" segment-id])
    (transmit
     {:journey (dissoc journey :receive)
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
  (println
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
  [{:keys [zookeeper zookeeper-connect]} {:keys [id topic]}]
  ;; (kafka/delete-topic zookeeper-connect topic)
  (zoo/delete zookeeper ["ibis" "journeys" id]))

(defn pull!
  [ibis {:keys [receive course] :as journey} reducer initial]
  (let [paths (course->paths course)]
    (loop [segment (receive)
           segments {}
           landed []]
      (let [segments (update segments (:segment-id segment) conj segment)
            landed (if (= :land (:message segment)) (conj landed segment) landed)]
        (if (all-landed? ibis journey paths segments landed segment)
          (let [landed-id (:segment-id (first landed))
                output-segments (mapv :message (apply concat (vals (dissoc segments landed-id))))
                results (reduce reducer initial output-segments)]
            (clean-up! ibis journey)
            results)
          (recur (receive) segments landed))))))
