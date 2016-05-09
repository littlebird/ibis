(ns ibis.flock
  (:require
   [clojure.pprint :as pprint]
   [taoensso.timbre :as log]
   [clj-time.core :as time]
   [com.climate.claypoole :as pool]
   [ibis.kafka :as kafka]))

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
    (output
      {:journey journey
       :stage :out
       :message message
       :traveled (conj traveled :out)
       :segment-id segment-id})))

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
        (let [exception (serialize-exception e)]
          (except exception "Exception in stage" stage stage-id)
          (update-fn
            :stage {:stage-id stage-id}
            {:failed (time/now)
             :exception exception
             :status "failed"})
          (passage
            transmit journey stage continuations
            {} traveled segment-id))))))

(defn launch!
  [{:keys [ibis-id transmit receive stages store producer encoders pool]
    update-fn :update
    :as ibis}]
  (let [flock-id (java.util.UUID/randomUUID)
        context (assoc ibis :update-fn update-fn :flock-id flock-id)]
    (log/trace "IBIS flock thread" flock-id "launched")
    (pool/future
      pool
      (loop [{:keys [journey stage message traveled segment-id]
              :as segment}
             (receive)]
        (when segment
          (try
            (if (= stage :out)
              (wrap-up-stage segment context)
              (when-let [work (get stages stage)]
                (run-stage segment work context)))
            (catch Exception e
              (let [exception (serialize-exception e)]
                (except exception "Exception during journey" (:id journey))))))
        (recur (receive))))))

(defn launch-all!
  [ibis n]
  (dotimes [_ n]
    (launch! ibis)))
