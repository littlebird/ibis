(ns ibis.zookeeper
  (:require
   [clojure.string :as string]
   [zookeeper :as zookeeper]
   [zookeeper.data :as data]))

(defn epoch
  []
  (.getTime (java.util.Date.)))

(defn pathify
  [path]
  (str \/ (string/join "/" path)))

(defn connect
  [host-port]
  (let [connection (zookeeper/connect host-port)]
    {:connection (atom connection)
     :host-port host-port}))

(defn reconnect
  [{:keys [host-port connection]}]
  (swap! connection (fn [old]
                      (zookeeper/close old)
                      (zookeeper/connect host-port))))

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
    zookeeper path))

(defn children
  [zookeeper path]
  (with-reconnect
    (fn [connection path]
      (zookeeper/children connection (pathify path)))
    zookeeper path))

(defn count-children
  [zookeeper path]
  (:numChildren (exists? zookeeper path)))

(defn create
  ([zookeeper path] (create zookeeper path {:persistent? true}))
  ([zookeeper path {:keys [persistent?]}]
   (doseq [span (range 1 (inc (count path)))]
     (let [subpath (take span path)]
       (if-let [info (exists? zookeeper subpath)]
         info
         (zookeeper/create
          @(:connection zookeeper)
          (pathify subpath)
          :persistent? persistent?))))))

(def data-map
  {:int data/to-int
   :long data/to-long
   :double data/to-double
   :char data/to-char
   :string data/to-string})

(defn get-data
  ([zookeeper path] (get-data zookeeper path :long))
  ([zookeeper path convert-key]
   (let [convert (get data-map convert-key data/to-long)
         data (with-reconnect
                (fn [connection path]
                  (zookeeper/data connection (pathify path)))
                zookeeper path)]
     (if-let [data (:data data)]
       (convert data)))))

(defn set-data
  [zookeeper path data]
  (let [info (or (exists? zookeeper path) (create zookeeper path))
        version (:version info)]
    (zookeeper/set-data
     @(:connection zookeeper)
     (pathify path)
     (data/to-bytes data)
     version)))

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
