(ns ibis.transit
  (:require
   [cognitect.transit :as transit]
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

(def encoders
  {:handlers
   {org.joda.time.DateTime time-writer
    java.sql.Timestamp time-writer}})

(def decoders
  {:handlers {}})

(defn kafka-serialize
  [segment]
  (let [baos (ByteArrayOutputStream. 512)
        writer (transit/writer baos :json encoders)
        _ (transit/write writer segment)]
    (.toByteArray baos)))

(defn kafka-deserialize
  [bytes]
  (let [bais (ByteArrayInputStream. bytes)
        reader (transit/reader bais :json decoders)]
    (transit/read reader)))

