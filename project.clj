(defproject aviary/ibis "0.0.7"
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
  :repl-options {:init-ns ibis.core}
  :plugins [[s3-wagon-private "1.1.2"]]
  :repositories ^:replace
  [["central" {:url "http://repo1.maven.org/maven2"}]
   ["clojure" {:url "http://build.clojure.org/releases"}]
   ["clojure-snapshots" {:url "http://build.clojure.org/snapshots"}]
   ["clojars" {:url "http://clojars.org/repo/"}]
   ["private" {:url "s3p://littlebird-maven/releases/"
               :creds :gpg
               :sign-releases false}]])
