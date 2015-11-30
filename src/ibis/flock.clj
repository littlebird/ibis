(ns ibis.flock
  (:require
   [clj-time.core :as time]
   [ibis.kafka :as kafka]))

(defn serialize-exception
  [exception]
  {:class (str (class exception))
   :message (.getMessage exception)
   :backtrace (map str (.getStackTrace exception))})

(defn launch!
  [{:keys [transmit receive stages store update producer]}]
  (let [flock-id (java.util.UUID/randomUUID)]
    (future
      (loop [{:keys [journey stage message traveled segment-id] :as segment} (receive)]
        (if (= stage :out)
          (let [output (kafka/make-transmit producer (:topic journey))]
            (output
             {:journey journey
              :stage :out
              :message message
              :traveled (conj traveled stage)
              :segment-id segment-id}))
          (let [work (get stages stage)
                stage-id (java.util.UUID/randomUUID)]
            (store
             :stage
             {:journey-id (:id journey)
              :stage stage
              :stage-id stage-id
              :started (time/now)})
            (try
              (let [result (if (= message :land) :land (work message))
                    continuations (get-in journey [:course stage])]
                (update
                 :stage {:stage-id stage-id}
                 {:completed (time/now)})
                (doseq [continuation continuations]
                  (transmit
                   {:journey journey
                    :stage continuation
                    :message result
                    :traveled (conj traveled stage)
                    :segment-id segment-id})))
              (catch Exception e
                (update
                 :stage {:stage-id stage-id}
                 {:failed (time/now) :exception (serialize-exception e)})))))
        (recur (receive))))))
