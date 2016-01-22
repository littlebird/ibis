(ns ibis.heartbeat
  (:require
   [ibis.zookeeper :as zoo]))

(def node-path ["ibis" "nodes"])
(def segment-keys [:journey :stage :message :traveled :segment-id])

(defn register-ibis
  [{:keys [zookeeper ibis-id]}]
  (zoo/create zookeeper (conj node-path ibis-id)))

(defn node-down?
  [now then beat-period]
  (> (- now then) (* 2 beat-period)))

(defn keywordize-course
  [course]
  (into
   {}
   (map
    (fn [[stage branches]]
      [stage (map keyword branches)])
    course)))

(defn deserialize-segment
  [segment]
  (let [relevant (select-keys segment segment-keys)
        staged (update relevant :stage keyword)
        coursed (update staged :course keywordize-course)
        traveled (update coursed :traveled (partial map keyword))]
    traveled))

(defn handle-node-down
  [{:keys [zookeeper transmit fetch] :as ibis} node-id]
  (println "NODE DOWN!" node-id)
  (zoo/delete zookeeper (conj node-path node-id))
  (let [incomplete
        (fetch
         :stage
         {:ibis-id (java.util.UUID/fromString node-id)
          :status "running"})]
    (doseq [segment incomplete]
      (println "INCOMPLETE SEGMENT" (:stage segment) " ---------- RELAUNCHING")
      (transmit
       (deserialize-segment segment)))))

(defn beat
  [{:keys [zookeeper ibis-id beat-period] :as ibis}]
  (try
    (let [info (zoo/set-data
                zookeeper (conj node-path ibis-id) 1)
          now (:mtime info)
          nodes (zoo/children zookeeper node-path)]
      (doseq [node nodes]
        (let [node-info (zoo/exists? zookeeper (conj node-path node))
              then (:mtime node-info)]
          (if (node-down? now then beat-period)
            (handle-node-down ibis node)))))
    (catch Exception e (println e))))
