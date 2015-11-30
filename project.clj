(defproject ibis "0.0.1"
  :description "Distribute jobs among any number of peers using Kafka"
  :url "http://github.com/littlebird/ibis"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.371"]
                 [com.cognitect/transit-clj "0.8.275"]
                 [clj-time "0.5.0"]
                 [clj-kafka "0.3.3"]
                 [noisesmith/component "0.2.5"]]
  :main ibis.core)
