(ns ibis.core
  (:require
   [noisesmith.component :as component]
   [ibis.zookeeper :as zoo]
   [ibis.kafka :as kafka]))

(defrecord Ibis
  [stages
   zookeeper-host zookeeper-port kafka-port
   group topic
   store update fetch
   complete
   producer-opts consumer-opts]

  component/Lifecycle

  (start [component]
    (let [zookeeper (zoo/connect zookeeper-host zookeeper-port)
          producer (kafka/make-producer zookeeper-host kafka-port producer-opts)
          consumer (kafka/make-consumer zookeeper-host zookeeper-port group consumer-opts)
          transmit (kafka/make-transmit producer topic)
          receive (kafka/make-receive consumer topic)]
      (zoo/create zookeeper ["ibis" "journeys"])
      (assoc
       component
       :zookeeper zookeeper
       :zookeeper-connect (str zookeeper-host \: zookeeper-port)
       :producer producer
       :consumer consumer
       :transmit transmit
       :receive receive)))

  (stop [component]))

(defn new-ibis
  [config]
  (map->Ibis config))

(defonce ibis (agent nil))

(defn ibis-in
  [segment]
  (println "in segment!" (keys segment))
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
   :store (fn [kind data]) ;(println "storing" kind)
   :update (fn [kind signature data]) ;(println "updating" kind signature)
   :fetch (fn [kind signature]) ;(println "fetching" kind signature)
   :complete (partial map (partial merge-with merge))})

(defn stop
  []
  (send ibis (fn [ibis] (when ibis (component/stop ibis)) nil))
  (await ibis))

(defn start
  [config]
  (println "IBIS starting...")
  (let [stages (merge (:stages default-config) (:stages config))
        config (assoc (merge default-config config) :stages stages)]
    (stop)
    (send ibis (constantly (new-ibis config)))
    (send ibis component/start)
    (await ibis)
    (println "IBIS started!")))

