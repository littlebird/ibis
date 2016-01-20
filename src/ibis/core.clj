(ns ibis.core
  (:require
   [com.stuartsierra.component :as component]
   [taoensso.timbre :as log]
   [ibis.zookeeper :as zoo]
   [ibis.kafka :as kafka]
   [ibis.tempo :as tempo]))

(defrecord Ibis
  [stages
   zookeeper-host zookeeper-port kafka-port
   group topic
   store update fetch
   complete
   encoders decoders
   producer-opts consumer-opts
   scheduler-threads
   beat-period]

  component/Lifecycle

  (start [component]
    (let [ibis-id (java.util.UUID/randomUUID)
          zookeeper (zoo/connect zookeeper-host zookeeper-port)
          producer (kafka/make-producer zookeeper-host kafka-port producer-opts)
          consumer (kafka/make-consumer zookeeper-host zookeeper-port group consumer-opts)
          transmit (kafka/make-transmit producer topic encoders)
          receive (kafka/make-receive consumer topic decoders)
          scheduler (tempo/new-scheduler scheduler-threads)
          ibis 
          (assoc
           component
           :ibis-id ibis-id
           :zookeeper zookeeper
           :zookeeper-connect (str zookeeper-host \: zookeeper-port)
           :producer producer
           :consumer consumer
           :transmit transmit
           :receive receive
           :schedule (partial tempo/periodically scheduler))]
      (zoo/create zookeeper ["ibis" "journeys"])
      (tempo/schedule scheduler (partial tempo/heartbeat ibis))
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
  (stop)
  (send ibis (constantly (start config)))
  (await ibis))
