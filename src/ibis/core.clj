(ns ibis.core
  (:require
   [clj-kafka.core :as kafka]
   [clj-kafka.zk :as zookeeper]
   [clj-kafka.producer :as producer]
   [clj-kafka.consumer.zk :as consumer]))

