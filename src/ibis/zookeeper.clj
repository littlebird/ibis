(ns ibis.zookeeper
  (:require
   [clojure.string :as string]
   [zookeeper :as zookeeper]))

(defn pathify
  [path]
  (str \/ (string/join "/" path)))

(defn establish-connection
  [host port]
  (zookeeper/connect (str host \: port)))

(defn connect
  [host port]
  (let [connection (establish-connection host port)]
    {:connection (atom connection)
     :host host
     :port port}))

(defn reconnect
  [{:keys [host port connection]}]
  (swap! connection (constantly (establish-connection host port))))

(defn with-reconnect
  [f zookeeper & args]
  (try
    (apply f (cons @(:connection zookeeper) args))
    (catch Exception e
      (reconnect zookeeper)
      (apply f (cons @(:connection zookeeper) args)))))

(defn exists?
  [zookeeper path]
  (with-reconnect
    (fn [connection path]
      (zookeeper/exists connection (pathify path)))
    zookeeper
    path))

(defn count-children
  [zookeeper path]
  (:numChildren (exists? zookeeper path)))

(defn create
  ([zookeeper path] (create zookeeper path {:persistent? true}))
  ([zookeeper path {:keys [persistent?]}]
   (doseq [span (range 1 (inc (count path)))]
     (let [subpath (take span path)]
       (if-not (exists? zookeeper subpath)
         (zookeeper/create
          @(:connection zookeeper)
          (pathify subpath)
          :persistent? persistent?))))))

(defn delete
  [zookeeper path]
  (with-reconnect
    (fn [connection path]
      (zookeeper/delete-all connection (pathify path)))
    zookeeper
    path))

(defn ibis-watcher
  [{:keys [event-type keeper-state path] :as event}]
  (println "IBIS watcher triggered!" (str event))
  (condp = event-type
    :NodeCreated nil
    :NodeDeleted nil
    :NodeChildrenChanged nil
    :NodeDataChanged nil
    nil))
