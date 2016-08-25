(ns ibis.heartbeat
  (:require
   [taoensso.timbre :as log]
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
  (log/info "NODE DOWN!" node-id)
  (zoo/delete zookeeper (conj node-path node-id))
  (let [incomplete
        (fetch
         :stage
         {:ibis-id (java.util.UUID/fromString node-id)
          :status "running"})]
    (doseq [segment incomplete]
      (log/info "INCOMPLETE SEGMENT" (:stage segment) "---------- RELAUNCHING")
      (transmit
       (deserialize-segment segment)))))

(defn beat
  [{:keys [zookeeper ibis-id beat-period] :as ibis}]
  (let [id-path (conj node-path ibis-id)]
    (try
      (when-not (zoo/exists? zookeeper id-path)
        (zoo/create zookeeper id-path))
      (let [info (zoo/set-data zookeeper id-path 1)
            now (:mtime info)
            nodes (zoo/children zookeeper node-path)]
        (doseq [node nodes]
          (let [node-info (zoo/exists? zookeeper (conj node-path node))
                then (when node-info
                       (:mtime node-info))]
            (if (and then
                     (node-down? now then beat-period))
              (handle-node-down ibis node)))))
      (catch Exception e
        (println ::beat "\n" (pr-str e))))))
