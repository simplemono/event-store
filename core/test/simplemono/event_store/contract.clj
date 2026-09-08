(ns simplemono.event-store.contract
  "Shared assertions run by each backend's test suite; not a runtime dependency."
  (:require [clojure.test :refer [is testing]]
            [simplemono.event-store :as event-store]))

(defn- outcome
  [f]
  (try (f) (catch Throwable t t)))

(defn positions!
  [make-store]
  (doseq [position [nil true false "0" :zero
                    -1 Long/MIN_VALUE
                    (int 0) (short 0) (byte 0)
                    0N 42N (biginteger 0) (biginteger 42) (inc' Long/MAX_VALUE)
                    0.0 0.9 -0.9 (float 0) 0M 1/2
                    Double/NaN Double/POSITIVE_INFINITY]]
    (testing (str "invalid position " (pr-str position) " of type " (type position))
      (let [s (make-store)]
        (doseq [[op f] [[:append #(event-store/try-append! s position :event)]
                       ;; Validation must happen at events(), not on reduction.
                       [:events #(event-store/events s position)]]]
          (testing (name op)
            (let [t (outcome f)]
              (is (instance? clojure.lang.ExceptionInfo t))
              (is (= :incorrect (:error (ex-data t)))))))
        (is (nil? (event-store/latest-event-number s)))
        (is (= [] (into [] (event-store/events s 0)))))))
  (testing "ordinary Clojure long literals and explicit Java longs are accepted"
    (let [s (make-store)]
      (is (true? (event-store/try-append! s 0 :first)))
      (is (true? (event-store/try-append! s (long 1) :second)))
      (is (= [:first :second] (into [] (event-store/events s (long 0)))))
      (is (instance? Long (event-store/latest-event-number s)))))
  (testing "Long/MAX_VALUE is valid, even when it is beyond an empty stream"
    (let [s (make-store)]
      (is (= [] (into [] (event-store/events s Long/MAX_VALUE))))
      (is (= :gap (:error (ex-data (outcome #(event-store/try-append! s Long/MAX_VALUE :event)))))))))

(defn nil-events!
  [make-store]
  (let [s (make-store)
        values [nil false {:event/type :example/happened} nil]]
    (doseq [[n value] (map-indexed vector values)]
      ;; map-indexed's index is a primitive long, boxed at the protocol boundary.
      (is (true? (event-store/try-append! s n value))))
    (is (= 3 (event-store/latest-event-number s)))
    (is (false? (event-store/try-append! s 0 nil)) "nil still occupies its slot")
    (is (= values (into [] (event-store/events s 0))))
    (is (= (subvec values 1) (into [] (event-store/events s 1))))
    (is (= [nil] (into [] (event-store/events s 3))))
    (is (= [] (into [] (event-store/events s 4))))
    (is (= [nil false] (into [] (take 2) (event-store/events s 0))))
    (let [seen (atom [])]
      (is (nil? (reduce (fn [_ value]
                         (swap! seen conj value)
                         (reduced nil))
                       :init (event-store/events s 0))))
      (is (= [nil] @seen) "a nil result from an early stop is not a missing event"))))

(defn long-limit!
  "Seed only the relevant tail: materializing the entire stream is impractical.
   The seeded store must support ordinary append, replay and head operations."
  [make-seeded-store]
  (doseq [length [1 2 102]]
    (testing (str "a tail of " length " events before Long/MAX_VALUE")
      (let [from (- Long/MAX_VALUE length)
            initial (into (sorted-map)
                          (map (fn [offset] [(+ from offset) offset]))
                          (range length))
            s (make-seeded-store initial)]
        (is (= (dec Long/MAX_VALUE) (event-store/latest-event-number s)))
        (is (true? (event-store/try-append! s Long/MAX_VALUE nil)))
        (is (false? (event-store/try-append! s Long/MAX_VALUE :duplicate)))
        (is (= Long/MAX_VALUE (event-store/latest-event-number s)))
        (is (= (conj (vec (range length)) nil)
               (into [] (event-store/events s from))))
        (is (= [nil] (into [] (event-store/events s Long/MAX_VALUE))))
        (is (= [nil] (into [] (take 1) (event-store/events s Long/MAX_VALUE))))))))
