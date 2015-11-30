(ns ibis.core-test
  (:require
   [clojure.test :refer :all]
   [ibis.flock :as flock]
   [ibis.journey :as journey]
   [ibis.core :as ibis]))

(def stages
  {:a (fn [{:keys [x]}] {:x (str x "-a")})
   :b (fn [{:keys [x]}] {:x (str x "-b")})
   :c (fn [{:keys [x]}] {:x (str x "-c")})
   :d (fn [{:keys [x]}] {:x (str x "-d")})
   :e (fn [{:keys [x]}] {:x (str x "-e")})})

(def course
  {:in [:a :b :c]
   :a [:d :e :out]
   :b [:d]
   :c [:out]
   :d [:e]
   :e [:out]})

(deftest ibis-test
  (testing "Ibis full circle"
    (let [ibis (ibis/start {:stages stages})]
      (flock/launch! ibis)
      (flock/launch! ibis)
      (flock/launch! ibis)
      (let [journey (journey/submit! ibis course)]
        (doseq [x (range 15)]
          (journey/push! ibis journey {:x x}))
        (journey/finish! ibis journey)
        (let [results (journey/pull! ibis journey conj [])]
          (println results)
          (is (= (count results) 75))))
      (ibis/stop ibis))))
