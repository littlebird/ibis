(ns ibis.heartbeat
  (:require
   [ibis.zookeeper :as zoo]))

(def node-path ["ibis" "nodes"])

(defn register-ibis
  [{:keys [zookeeper ibis-id]}]
  (zoo/create zookeeper (conj node-path ibis-id)))

(defn node-down?
  [now then beat-period]
  (> (- now then) (* 2 beat-period)))

(defn handle-node-down
  [{:keys [zookeeper transmit fetch] :as ibis} node-id]
  (zoo/delete zookeeper (conj node-path node-id))
  (let [incomplete (fetch :stage {:ibis-id node-id :status "running"})]
    (doseq [segment incomplete]
      (transmit
       (select-keys
        segment
        [:journey :stage :message :traveled :segment-id])))))

(defn beat
  [{:keys [zookeeper ibis-id beat-period] :as ibis}]
  (let [info (zoo/set-data
              zookeeper (conj node-path ibis-id) 1)
        now (:mtime info)
        nodes (zoo/children zookeeper node-path)]
    (doseq [node nodes]
      (let [node-info (zoo/exists? zookeeper (conj node-path node))
            then (:mtime node-info)]
        (if (node-down? now then beat-period)
          (handle-node-down ibis node))))))
