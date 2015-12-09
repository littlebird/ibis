(ns ibis.core-test
  (:require
   [clojure.test :refer :all]
   [ibis.flock :as flock]
   [ibis.journey :as journey]
   [ibis.core :as ibis]))

;; kafka and zookeeper must be running for these tests to pass!

(def stages
  {:a (fn [{:keys [x]}] {:x (str x "-a")})
   :b (fn [{:keys [x]}] {:x (str x "-b")})
   :c (fn [{:keys [x]}] {:x (str x "-c")})
   :d (fn [{:keys [x]}] {:x (str x "-d")})
   :e (fn [{:keys [x]}] {:x (str x "-e")})

   :inc (fn [m] (update m :n inc))
   :add-five (fn [m] (update m :n (partial + 5)))
   :str (fn [{:keys [n]}] {:s (str n)})})

(def course
  {:in [:a :b :c]
   :a [:d :e :out]
   :b [:d]
   :c [:out]
   :d [:e]
   :e [:out]})

(deftest ibis-one-journey
  (testing "Ibis full circle"
    (let [ibis (ibis/start {:stages stages})]
      (flock/launch-all! ibis 3)
      (let [journey (journey/submit! ibis course)]
        (doseq [x (range 15)]
          (journey/push! ibis journey {:x x}))
        (journey/finish! ibis journey)
        (let [results (journey/pull! ibis journey conj [])]
          (println results)
          (is (= (count results) 75))))
      (ibis/stop ibis))))

(deftest ibis-multiple-journeys
  (testing "Ibis full circle"
    (let [ibis (ibis/start {:stages stages})]
      (flock/launch-all! ibis 3)
      (let [journey-a (journey/submit! ibis course)]
        (doseq [x (range 5)]
          (journey/push! ibis journey-a {:x x}))
        (let [journey-b (journey/submit! ibis course)]
          (doseq [x (range 5 15)]
            (journey/push! ibis journey-a {:x x})
            (journey/push! ibis journey-b {:x x}))
          (journey/finish! ibis journey-a)
          (doseq [x (range 15 20)]
            (journey/push! ibis journey-b {:x x}))
          (let [results-a (journey/pull! ibis journey-a conj [])]
            (println results-a)
            (is (= (count results-a) 75)))
          (journey/finish! ibis journey-b)
          (let [results-b (journey/pull! ibis journey-b conj [])]
            (println results-b)
            (is (= (count results-b) 75)))))
      (ibis/stop ibis))))

(def branching-course
  {:in [:inc :add-five]
   :inc [:str]
   :add-five [:str]
   :str [:out]})

(deftest ibis-readme-journey
  (testing "Example from README"
    (let [ibis (ibis/start {:stages stages})]
      (flock/launch-all! ibis 3)
      (let [journey (journey/submit! ibis branching-course)]
        (doseq [part [{:n 1} {:n 5} {:n 88}]]
          (journey/push! ibis journey part))
        (journey/finish! ibis journey)
        (let [results (journey/pull! ibis journey conj [])]
          (println results)
          (is (= (count results) 6))))
      (ibis/stop ibis))))
