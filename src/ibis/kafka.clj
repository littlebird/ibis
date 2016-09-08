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

(defn receiver
  [consumer topic ready receive stream it cleanup]
  (swap! receivers conj
         {:started (java.util.Date.)
          :topic topic
          :consumer consumer
          :ready ready
          :receive receive
          :stream stream
          :get-it #(do it)
          :cleanup cleanup})
  (fn ibis-receive
    ([action]
     (cond (= action :close)
           (force cleanup)
           :else
           (log/error ::ibis-receive "unknown action" action)))
    ([]
     (>/>!! ready :ready)
     (if-let [result (>/<!! receive)]
       result
       (do (log/info ::ibis-receive "exiting" topic "as requested")
           (force cleanup))))))

(defn make-receive
  [consumer topic decoders]
  (let [ready (>/chan 1000)
        receive (>/chan 1000)
        stream (consumer/create-message-stream consumer topic)
        it (.iterator stream)
        clean-up (delay
                  (>/close! ready)
                  (>/close! receive)
                  (swap! receivers
                         (partial filterv #(not= ((juxt :topic :consumer) %)
                                                 [topic consumer])))
                  :exit)
        continue? (fn []
                    (not (realized? clean-up)))]
    (future
      (try
        (loop []
          (let [request (>/<!! ready)]
            (if-not (and (= request :ready)
                         (continue?))
              (log/warn ::make-receive "leaving receive loop for" topic)
              (try (let [message (.message (.next it))
                         payload (transit/kafka-deserialize message decoders)]
                     (>/>!! receive payload))
                   (catch Throwable e
                     (log/error "Throw during receive loop!" e)
                     (throw e))
                   (catch Exception e
                     (log/error "Exception during receive loop!" e)
                     (>/>!! receive {:error "receive loop failure"
                                     :topic topic})
                     (force clean-up))))
            (when (continue?)
              (recur))))
        (finally (force clean-up))))
    (receiver consumer topic ready receive stream it clean-up)))

(defn create-topic
  [zookeeper topic partitions]
  (with-open [zk (admin/zk-client zookeeper)]
    (admin/create-topic zk topic {:partitions partitions})))

(defn delete-topic
  [zookeeper topic]
  (with-open [zk (admin/zk-client zookeeper)]
    (admin/delete-topic zk topic)))
