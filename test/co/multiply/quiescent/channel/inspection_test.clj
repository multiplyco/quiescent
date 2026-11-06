(ns co.multiply.quiescent.channel.inspection-test
  (:require [clojure.test :refer [deftest is testing]]
    [co.multiply.quiescent.channel :as ch])
  (:import [co.multiply.quiescent.impl.channel BoundedChannel]))


(defn- field
  [name]
  (doto (.getDeclaredField BoundedChannel name) (.setAccessible true)))


(def ^:private put-sequence-field (field "publishedPutSequence"))
(def ^:private take-sequence-field (field "publishedTakeSequence"))


(deftest sampled-count-is-bounded-before-integer-conversion
  ;; Feed count the pair of sequence observations it could obtain across a pause.
  ;; These are not simultaneous valid ring states, so this fixture performs only
  ;; inspection. It exercises arithmetic boundaries without a production test hook
  ;; or billions of transfers; it does not force the concurrent sampling race.
  (doseq [[label produced consumed expected]
          [["consumption passes the earlier production observation" 0 1 0]
           ["a negative gap beyond int range must not become a positive count"
            0 (+ (long Integer/MAX_VALUE) 2) 0]
           ["a long-wrapped sampling gap must still respect capacity"
            0 (inc Long/MIN_VALUE) 8]]]
    (testing label
      (let [channel (ch/chan 8)]
        (.setLong ^java.lang.reflect.Field put-sequence-field channel produced)
        (.setLong ^java.lang.reflect.Field take-sequence-field channel consumed)
        (is (= expected (ch/buf-count channel)))
        (is (= (/ (double expected) 8) (ch/saturation channel)))))))
