(ns ibis.zookeeper
  (:require
   [clojure.string :as string]
   [zookeeper :as zookeeper]))

(defn pathify
  [path]
  (str \/ (string/join "/" path)))

(defn connect
  [host port]
  (zookeeper/connect
   (str host \: port)))

(defn exists?
  [zookeeper path]
  (zookeeper/exists zookeeper (pathify path)))

(defn count-children
  [zookeeper path]
  (:numChildren (exists? zookeeper path)))

(defn create
  [zookeeper path]
  (doseq [span (range 1 (inc (count path)))]
    (let [subpath (take span path)]
      (if-not (exists? zookeeper subpath)
        (zookeeper/create zookeeper (pathify subpath) :persistent? true)))))

(defn delete
  [zookeeper path]
  (zookeeper/delete-all zookeeper (pathify path)))

(defn ibis-watcher
  [{:keys [event-type keeper-state path] :as event}]
  (println "IBIS watcher triggered!" (str event))
  (condp = event-type
    :NodeCreated nil
    :NodeDeleted nil
    :NodeChildrenChanged nil
    :NodeDataChanged nil
    nil))
