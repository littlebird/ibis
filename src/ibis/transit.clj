(ns ibis.transit
  (:require
   [cognitect.transit :as transit]
   [timbre.log :as timbre]
   [clj-time.coerce :as coerce])
  (:import
   (java.io ByteArrayInputStream ByteArrayOutputStream)))

(def time-writer
  (transit/write-handler
   (constantly "m")
   #(-> % coerce/to-date .getTime)
   #(-> % coerce/to-date .getTime .toString)))

(def string-writer
  (transit/write-handler
   (constantly "'")
   str str))

(def default-encoders
  {:handlers
   {org.joda.time.DateTime time-writer
    java.sql.Timestamp time-writer}})

(def default-decoders
  {:handlers {}})

(defn kafka-serialize
  [segment encoders]
  (let [baos (ByteArrayOutputStream. 512)
        writer (transit/writer baos :json (update default-encoders :handlers merge encoders))]
    (try (transit/write writer segment)
         (.toByteArray baos)
         (catch Exception e
           (timbre/error "error encoding" (pr-str segment))
           (throw (ex-info (.getMessage e) {:segment segment}))))))

(defn kafka-deserialize
  [bytes decoders]
  (let [bais (ByteArrayInputStream. bytes)
        reader (transit/reader bais :json (update default-decoders :handlers merge decoders))]
    (transit/read reader)))

(defn read-topic
  [dump]
  (let [bytes (slurp dump)
        reader (transit/reader (java.io.ByteArrayInputStream. (.getBytes bytes)) :json {})
        segments (take-while identity (map (fn [x] (try (transit/read reader) (catch Exception e nil))) (range)))
        groups (group-by (comp str :segment-id) segments)]
    groups))
