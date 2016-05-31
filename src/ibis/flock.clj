(ns ibis.flock
  (:require
   [clojure.pprint :as pprint]
   [taoensso.timbre :as log]
   [clj-time.core :as time]
   [com.climate.claypoole :as pool]
   [ibis.zookeeper :as zk]
   [ibis.kafka :as kafka]))

;;; logging
(defn serialize-exception
  [exception]
  {:class (str (class exception))
   :message (.getMessage exception)
   :backtrace (map str (.getStackTrace exception))})

(defn except
  [error & message]
  (locking *out*
    (log/error (apply print-str message)
               \newline
               (with-out-str (pprint/pprint error)))))

;;; flock segment counts
(def ibis-population-path
  ["ibis" "flock" "population"])

(defonce ibis-count (atom 0))

(defn update-population
  [zookeeper flock-id]
  (try
    (zk/set-data zookeeper (conj ibis-population-path flock-id) @ibis-count)
    (catch Exception e
      (except (assoc (serialize-exception e) :population @ibis-count) ::update-population))))

(defn increase-flock
  [zookeeper flock-id]
  (swap! ibis-count inc)
  (update-population zookeeper flock-id))

(defn decrease-flock
  [zookeeper flock-id]
  (swap! ibis-count dec)
  (update-population zookeeper flock-id))

(defn flock-headcount
  [zookeeper]
  (let [children (zk/children zookeeper ibis-population-path)]
    (if (sequential? children)
      (apply +
             (for [child children]
               (zk/with-reconnect
                 (fn [connection]
                   (or (zk/get-data zookeeper (conj ibis-population-path child) :long)
                       0))
                 zookeeper)))
      0)))

(defn start-counted-flock
  [zookeeper flock-id]
    (zk/create zookeeper ibis-population-path)
    (zk/create zookeeper (conj ibis-population-path flock-id)
               {:persistent? false}))

;;; stage-running
(defn passage
  [transmit journey stage continuations result traveled segment-id]
  (doseq [continuation continuations]
    (transmit
     {:journey journey
      :stage continuation
      :message result
      :traveled (conj traveled stage)
      :segment-id segment-id})))

(defn wrap-up-stage
  [{:keys [journey message traveled segment-id]
    :as segment}
   {:keys [encoders producer]
    :as ibis}]
  (let [output (kafka/make-transmit producer (:topic journey) encoders)]
    (output {:journey journey
             :stage :out
             :message message
             :traveled (conj traveled :out)
             :segment-id segment-id})))

(defn fail-stage
  [e
   {:keys [stage traveled journey segment-id stage-id continuations]
    :as segment}
   {:keys [update-fn transmit]
    :as ibis}]
  (let [exception (serialize-exception e)]
    (except exception "Exception in stage" stage stage-id)
    (update-fn
      :stage {:stage-id stage-id}
      {:failed (time/now)
       :exception exception
       :status "failed"})
    (passage
      transmit journey stage continuations
      {} traveled segment-id)))

(defn run-stage
  [{:keys [message stage traveled journey segment-id]
    :as segment}
   work
   {:keys [ibis-id transmit store update-fn]
    :as ibis}]
  (let [stage-id (java.util.UUID/randomUUID)
        continuations (get-in journey [:course stage])]
    (store
      :stage
      (merge
        segment
        {:ibis-id ibis-id
         :journey-id (:id journey)
         :stage stage
         :stage-id stage-id
         :started (time/now)
         :status "running"}))
    (try
      (let [result
            (if (= message :land)
              :land
              (work (assoc message :ibis ibis)))]
        (update-fn
          :stage {:stage-id stage-id}
          {:completed (time/now)
           :status "complete"})
        (passage
          transmit journey stage continuations
          (if (keyword? result)
            result
            (dissoc result :ibis))
          traveled segment-id))
      (catch Exception e
        (fail-stage e
                    (assoc segment
                           :stage-id stage-id
                           :continuations continuations)
                    ibis)))))

(defn launch!
  [{:keys [receive stages pool zookeeper]
    update-fn :update
    :as ibis}]
  (let [flock-id (java.util.UUID/randomUUID)
        context (assoc ibis :update-fn update-fn :flock-id flock-id)]
    (start-counted-flock zookeeper flock-id)
    (log/trace "IBIS flock thread" flock-id "launched")
    (pool/future
      pool
      (loop [{:keys [journey stage message traveled segment-id]
              :as segment}
             (receive)]
        (when segment
          (increase-flock zookeeper flock-id)
          (try
            (if (= stage :out)
              (wrap-up-stage segment context)
              (when-let [work (get stages stage)]
                (run-stage segment work context)))
            (catch Exception e
              (let [exception (serialize-exception e)]
                (except exception "Exception during journey" (:id journey))))
            (finally (decrease-flock zookeeper flock-id))))
        (recur (receive))))))

(defn launch-all!
  [ibis n]
  (dotimes [_ n]
    (launch! ibis)))
