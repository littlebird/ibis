(ns ibis.tempo
  (:require
   [clj-time.core :as time]
   [taoensso.timbre :as log]
   [ibis.zookeeper :as zoo]
   [ibis.journey :as journey])
  (:import
   [java.util.concurrent ScheduledThreadPoolExecutor TimeUnit]))

(defn new-scheduler
  [scheduler-threads]
  (ScheduledThreadPoolExecutor. scheduler-threads))

(defn milliseconds-between
  [a b]
  (let [interval (time/interval a b)]
    (time/in-msecs interval)))

(defn milliseconds-of
  [interval]
  (.getMillis (.toStandardDuration (.toPeriod interval))))

(defn next-time
  ([hour] (next-time hour 0))
  ([hour minute]
   (let [target (time/today-at hour minute)
         now (time/now)]
     (if (time/before? target now)
       (time/plus target (time/days 1))
       target))))

(defn periodic-submit
  ([ibis course f] (periodic-submit ibis course f nil))
  ([ibis course f chan]
   (log/info ::periodic-submit "JOURNEY TRIGGERED" course)
   (let [cleanup (promise)
         journey (journey/submit! ibis course cleanup)]
     (f (partial journey/push! ibis journey))
     (journey/finish! ibis journey)
     (future (journey/pull! ibis journey conj [] chan)
             (when (realized? cleanup)
               (@cleanup))))))

(defn schedule
  ([scheduler f period] (schedule scheduler f period 0))
  ([scheduler f period delay]
   (.scheduleAtFixedRate scheduler f delay period TimeUnit/MILLISECONDS)))

(defn periodically
  ([scheduler ibis when interval course f]
   (periodically scheduler ibis when interval course f nil))
  ([scheduler ibis when interval course f chan]
   (let [delay (milliseconds-between (time/now) when)
         period (milliseconds-of interval)
         work (partial periodic-submit ibis course f chan)
         task (schedule scheduler work period delay)]
     #(.cancel task true))))

(defn uniquely!
  ;; this is racey, can be fixed with compare+set
  [{:keys [zookeeper]} path f]
  (when-not (zoo/exists? zookeeper path)
    (zoo/create zookeeper path {:persistent? false})
    (log/info "UNIQUELY" path)
    (f)))
