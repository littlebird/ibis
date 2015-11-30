(ns ibis.kafka
  (:require
   [clojure.core.async :as >]
   [clj-kafka.core :as kafka]
   [clj-kafka.zk :as zookeeper]
   [clj-kafka.new.producer :as producer]
   [clj-kafka.consumer.zk :as consumer]
   [clj-kafka.admin :as admin]
   [ibis.transit :as transit]))

(defn make-producer
  [zookeeper-host kafka-port opts]
  (producer/producer
   (merge
    {"bootstrap.servers" (str zookeeper-host \: kafka-port)}
    opts)
   (producer/byte-array-serializer)
   (producer/byte-array-serializer)))

(defn make-consumer
  [zookeeper-host zookeeper-port group opts]
  (consumer/consumer
   (merge
    {"zookeeper.connect" (str zookeeper-host \: zookeeper-port)
     "group.id" group
     "fetch.message.max.bytes" "8320000"
     "auto.offset.reset" "largest"
     "auto.commit.interval.ms" "200"
     "auto.commit.enable" "true"}
    opts)))

(defn make-transmit
  [producer topic encoders]
  (fn ibis-transmit
    [message]
    (let [baos (transit/kafka-serialize message encoders)
          encoded (producer/record topic baos)]
      (producer/send producer encoded))))

(defn make-receive
  [consumer topic decoders]
  (let [ready (>/chan 1000)
        receive (>/chan 1000)
        stream (consumer/create-message-stream consumer topic)
        it (.iterator stream)]
    (>/go
      (>/<! ready)
      (loop [message (.message (.next it))]
        (let [payload (transit/kafka-deserialize message decoders)]
          (>/>! receive payload)
          (>/<! ready))
        (recur (.message (.next it)))))
    (fn ibis-receive
      []
      (>/>!! ready :ready)
      (>/<!! receive))))

(defn create-topic
  [zookeeper topic partitions]
  (with-open [zk (admin/zk-client zookeeper)]
    (admin/create-topic zk topic {:partitions partitions})))

(defn delete-topic
  [zookeeper topic]
  (with-open [zk (admin/zk-client zookeeper)]
    (admin/delete-topic zk topic)))
