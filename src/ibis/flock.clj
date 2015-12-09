(ns ibis.flock
  (:require
   [clojure.pprint :as pprint]
   [taoensso.timbre :as log]
   [clj-time.core :as time]
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
  [{:keys [transmit receive stages store update producer encoders]}]
  (let [flock-id (java.util.UUID/randomUUID)]
    (future
      (loop [{:keys [journey stage message traveled segment-id] :as segment} (receive)]
        (try
          (if segment
            (if (= stage :out)
              (let [output (kafka/make-transmit producer (:topic journey) encoders)]
                (output
                 {:journey journey
                  :stage :out
                  :message message
                  :traveled (conj traveled stage)
                  :segment-id segment-id}))
              (let [work (get stages stage)
                    stage-id (java.util.UUID/randomUUID)
                    continuations (get-in journey [:course stage])]
                (store
                 :stage
                 {:journey-id (:id journey)
                  :stage stage
                  :stage-id stage-id
                  :started (time/now)})
                (try
                  (let [result (if (= message :land) :land (work message))]
                    (update
                     :stage {:stage-id stage-id}
                     {:completed (time/now)})
                    (passage
                     transmit journey stage continuations
                     result traveled segment-id))
                  (catch Exception e
                    (let [exception (serialize-exception e)]
                      (log/error "Exception in stage" stage stage-id)
                      (log/error (pprint/pprint exception))
                      (update
                       :stage {:stage-id stage-id}
                       {:failed (time/now) :exception exception})
                      (passage
                       transmit journey stage continuations
                       {} traveled segment-id)))))))
          (catch Exception e
            (let [exception (serialize-exception e)]
              (log/error "Exception during journey" (:id journey))
              (log/error (pprint/pprint exception)))))
        (recur (receive))))))

(defn launch-all!
  [ibis n]
  (dotimes [_ n]
    (launch! ibis)))
