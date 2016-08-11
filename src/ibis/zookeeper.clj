(ns ibis.zookeeper
  (:require [clojure.string :as string]
            [zookeeper :as zookeeper]
            [zookeeper.data :as data])
  (:import (java.util.concurrent.locks ReentrantLock)
           (org.apache.zookeeper KeeperException$SessionExpiredException
                                 KeeperException$ConnectionLossException)))

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

(def reconnect-lock
  (ReentrantLock.))

(defonce reconnect-hooks
  (atom {}))

(defn reconnect
  [{:keys [host-port connection]}]
  (when (.tryLock reconnect-lock)
    (try
      (swap! connection (fn [old]
                          (zookeeper/close old)
                          (zookeeper/connect host-port)))
      (doseq [[k hook] @reconnect-hooks]
        (try (hook)
             (catch Exception e
               (println ::reconnect
                        "BAD HOOK" k \newline
                        (pr-str e)))))
      (finally (.unlock reconnect-lock)))))



(defn with-reconnect
  ([f zookeeper & args]
   (let [result (try
                  (apply f @(:connection zookeeper) args)
                  (catch KeeperException$SessionExpiredException _
                    (println ::with-reconnect "renegotiating expired session")
                    (reconnect zookeeper)
                    ::retry))]
     (if (= result ::retry)
       (recur f zookeeper args)
       result))))

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
   (doseq [subpath (rest (reductions conj [] path))]
     (when-not (exists? zookeeper subpath)
       (zookeeper/create
         @(:connection zookeeper)
         (pathify subpath)
         :persistent? persistent?)))))

(def data-map
  {:int data/to-int
   :long data/to-long
   :double data/to-double
   :char data/to-char
   :string data/to-string})

(defn convert
  [which data]
  (let [converter (get data-map which data/to-long)]
     (if-let [data (:data data)]
       (converter data))))

(defn get-raw
  [zookeeper path]
  (with-reconnect
    (fn [connection path]
      (zookeeper/data connection (pathify path)))
    zookeeper path))

(defn get-data
  ([zookeeper path] (get-data zookeeper path :long))
  ([zookeeper path convert-key]
   (convert convert-key (get-raw zookeeper path))))

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

(defn compare+set
  ([zookeeper path nxt]
   (compare+set zookeeper path nxt (get-raw zookeeper path)))
  ([zookeeper path nxt prev]
    (with-reconnect
      (fn [connection]
        (zookeeper/compare-and-set-data
          connection
          (pathify path)
          (:data prev)
          (data/to-bytes nxt)))
      zookeeper)))

(defn atomically
  ([zookeeper path f convert-key]
   (atomically zookeeper path f convert-key 100))
  ([zookeeper path f convert-key retries]
   (when (pos? retries)
     (let [state (get-raw zookeeper path)
           data (convert convert-key state)
           new-data (f data)
           success (compare+set zookeeper path new-data state)]
       (if success
         success
         (recur zookeeper path f convert-key (dec retries)))))))
