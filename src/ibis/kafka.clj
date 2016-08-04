(ns ibis.kafka
  (:require
   [clojure.core.async :as >]
   [taoensso.timbre :as log]
   [clj-kafka.core :as kafka]
   [clj-kafka.zk :as zookeeper]
   [clj-kafka.new.producer :as producer]
   [clj-kafka.consumer.zk :as consumer]
   [clj-kafka.admin :as admin]
   [ibis.transit :as transit]))

(defn make-producer
  [kafka-string opts]
  (producer/producer
   (merge
    {"bootstrap.servers" kafka-string
     "max.request.size" "8320000"}
    opts)
   (producer/byte-array-serializer)
   (producer/byte-array-serializer)))

(defn make-consumer
  [zookeeper-string group opts]
  (consumer/consumer
   (merge
    {"zookeeper.connect" zookeeper-string
     "group.id" group
     "fetch.message.max.bytes" "8320000"
     "auto.offset.reset" "largest"
     "auto.commit.interval.ms" "200"
     "auto.commit.enable" "true"}
    opts)))

(defn shutdown-consumer
  [consumer]
  (consumer/shutdown consumer))

(defn make-transmit
  [producer topic encoders]
  (fn ibis-transmit
    [message]
    (let [baos (transit/kafka-serialize message encoders)
          encoded (producer/record topic baos)]
      (producer/send producer encoded))))

(def receivers (atom []))

(def receiver
  [consumer topic ready receive stream it]
  (swap! receivers conj
         {:started (java.util.Date.)
          :topic topic
          :consumer consumer
          :ready ready
          :receive receive
          :stream stream
          :it it})
  (fn ibis-receive
    ([]
     (try
      (>/>!! ready :ready)
      (>/<!! receive)
      (catch Exception e
        (log/error "Exception in receive!" e))))))

(defn make-receive
  [consumer topic decoders]
  (let [ready (>/chan 1000)
        receive (>/chan 1000)
        stream (consumer/create-message-stream consumer topic)
        it (.iterator stream)]
    (>/go
      (>/<! ready)
      (loop [message (.message (.next it))]
        (try
          (let [payload (transit/kafka-deserialize message decoders)]
            (>/>! receive payload)
            (>/<! ready))
          (catch Exception e
            (log/error "Exception during receive loop!" e)))
        (recur (.message (.next it)))))
    (receiver ready receive stream it)))

(defn create-topic
  [zookeeper topic partitions]
  (with-open [zk (admin/zk-client zookeeper)]
    (admin/create-topic zk topic {:partitions partitions})))

(defn delete-topic
  [zookeeper topic]
  (with-open [zk (admin/zk-client zookeeper)]
    (admin/delete-topic zk topic)))
