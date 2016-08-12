(ns ibis.core
  (:require
   [com.stuartsierra.component :as component]
   [taoensso.timbre :as log]
   [com.climate.claypoole :as pool]
   [ibis.zookeeper :as zoo]
   [ibis.kafka :as kafka]
   [ibis.tempo :as tempo]
   [ibis.heartbeat :as heartbeat]))

(defrecord Ibis
           [stages
            zookeeper-host zookeeper-port kafka-port
            zookeeper-string
            kafka-string
            group topic
            store update fetch
            complete
            encoders decoders
            producer-opts consumer-opts
            flock-threads scheduler-threads
            beat-period]

  component/Lifecycle

  (start [component]
    (let [ibis-id (java.util.UUID/randomUUID)
          zk-string (or zookeeper-string (str zookeeper-host \: zookeeper-port))
          kafka-string (or kafka-string (str zookeeper-host \: kafka-port))
          zookeeper (zoo/connect zk-string)
          producer (kafka/make-producer kafka-string producer-opts)
          consumer (kafka/make-consumer zk-string group consumer-opts)
          transmit (kafka/make-transmit producer topic encoders)
          receive (kafka/make-receive consumer topic decoders)
          scheduler (tempo/new-scheduler scheduler-threads)
          pool (pool/threadpool flock-threads)
          ibis
          (assoc
           component
           :ibis-id ibis-id
           :zookeeper zookeeper
           :zookeeper-connect zk-string
           :zookeeper-string zk-string
           :kafka-string kafka-string
           :producer producer
           :consumer consumer
           :transmit transmit
           :receive receive
           :pool pool
           :schedule (partial tempo/periodically scheduler))]
      (log/trace "IBIS starting" flock-threads "threads")
      (zoo/create zookeeper ["ibis" "journeys"])
      (tempo/schedule scheduler (partial heartbeat/beat ibis) 30000 0)
      ibis))

  (stop [component]))

(defn ibis-in
  [segment]
  (log/trace "in segment!" (count segment))
  segment)

(def default-stages
  {:in ibis-in})

(def default-config
  {:stages default-stages
   :zookeeper-host "127.0.0.1"
   :zookeeper-port "2181"
   :kafka-port "9092"
   :group "ibis"
   :topic "ibis-journeys"
   :store (fn [kind data]) ;; (println "storing" kind)
   :update (fn [kind signature data]) ;; (println "updating" kind signature)
   :fetch (fn [kind signature]) ;; (println "fetching" kind signature)
   :complete (partial map (partial merge-with merge))
   :encoders {}
   :decoders {}
   :producer-opts {}
   :consumer-opts {}
   :flock-threads 20
   :scheduler-threads 20
   :beat-period 30000})

(defn new-ibis
  [config]
  (let [stages (merge (:stages default-config) (:stages config))
        config (assoc (merge default-config config) :stages stages)]
    (map->Ibis config)))

(defn stop
  [ibis]
  (when ibis (component/stop ibis)))

(defn start
  [config]
  (log/info "IBIS starting...")
  (let [ibis (new-ibis config)
        ibis (component/start ibis)]
    (log/info "IBIS started!")
    ibis))

(defonce ibis (agent nil))

(defn stop-ibis
  []
  (send ibis stop)
  (await ibis))

(defn start-ibis
  [config]
  (send ibis stop)
  (send ibis (constantly (start config)))
  (await ibis))
