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

(defn passage
  [transmit journey stage continuations result traveled segment-id]
  (doseq [continuation continuations]
    (transmit
     {:journey journey
      :stage continuation
      :message result
      :traveled (conj traveled stage)
      :segment-id segment-id})))

(defn launch!
  [{:keys [ibis-id transmit receive stages store updater producer encoders pool]
    :as ibis}]
  (let [flock-id (java.util.UUID/randomUUID)]
    (log/trace "IBIS flock thread" flock-id "launched")
    (pool/future
      pool
      (loop [{:keys [journey stage message traveled segment-id] :as segment} (receive)]
        (when segment
          (let [out-stage? (= stage :out)
                work (get stages stage)]
            (try
              (cond out-stage?
                    (let [output (kafka/make-transmit producer (:topic journey) encoders)]
                      (output
                        {:journey journey
                         :stage :out
                         :message message
                         :traveled (conj traveled stage)
                         :segment-id segment-id}))
                    work
                    (let [stage-id (java.util.UUID/randomUUID)
                          continuations (get-in journey [:course stage])]
                      (store
                        :stage
                        (assoc segment
                               :ibis-id ibis-id
                               :journey-id (:id journey)
                               :stage stage
                               :stage-id stage-id
                               :started (time/now)
                               :status "running"))
                      (try
                        (let [result
                              (if (= message :land)
                                :land
                                (work (assoc message :ibis ibis)))]
                          (updater :stage {:stage-id stage-id}
                                   {:completed (time/now)
                                    :status "complete"})
                          (passage
                            transmit journey stage continuations
                            (if (keyword? result)
                              result
                              (dissoc result :ibis))
                            traveled segment-id))
                        (catch Exception e
                          (locking *out*
                            (let [exception (serialize-exception e)]
                              (log/error "Exception in stage" stage stage-id)
                              (log/error (with-out-str (pprint/pprint exception)))
                              (updater :stage
                                       {:stage-id stage-id}
                                       {:failed (time/now)
                                        :exception exception
                                        :status "failed"})
                              (passage
                                transmit journey stage continuations
                                {} traveled segment-id)))))))
              (catch Exception e
                (locking *out*
                  (let [exception (serialize-exception e)]
                    (log/error "Exception during journey" (:id journey))
                    (log/error (with-out-str (pprint/pprint exception)))))))))
        (recur (receive))))))

(defn launch-all!
  [ibis n]
  (dotimes [_ n]
    (launch! ibis)))
