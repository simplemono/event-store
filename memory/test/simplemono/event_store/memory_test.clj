(ns simplemono.event-store.memory-test
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory :as memory]
            [simplemono.event-store.util :as util]))

(defn- event
  [n]
  {:event/occurred-at #inst "2026-08-25T00:00:00.000-00:00"
   :event/type :example/happened
   :event/n n})

(deftest appends-are-create-only-and-gap-free
  (let [s (memory/store)]
    (is (nil? (memory/latest-event-number s)))
    (testing "a number beyond the head would leave a hole"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Append would create a gap"
                            (event-store/try-append! s 1 (event 1)))))
    (testing "negative numbers are rejected"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Event numbers are zero-based"
                            (event-store/try-append! s -1 (event 0)))))
    (is (true? (event-store/try-append! s 0 (event 0))))
    (testing "losing the race is a false, not an exception"
      (is (false? (event-store/try-append! s 0 (event 0)))))
    (is (true? (event-store/try-append! s 1 (event 1))))
    (is (= 1 (memory/latest-event-number s)))
    (is (= [(event 0) (event 1)] (into [] (event-store/events s 0))))
    (is (= [(event 1)] (into [] (event-store/events s 1))))
    (is (= [] (into [] (event-store/events s 2))))))

(deftest the-stream-is-the-atom
  (let [state (atom (sorted-map))
        s (memory/store state)]
    (event-store/try-append! s 0 (event 0))
    (event-store/try-append! s 1 (event 1))
    (is (= {0 (event 0) 1 (event 1)} @state))
    (testing "a second store over the same atom sees the same stream"
      (is (= 1 (memory/latest-event-number (memory/store state)))))))

(deftest concurrent-appends-produce-one-winner-per-number
  (let [s (memory/store)
        writers 8
        results (->> (range writers)
                     (mapv (fn [n]
                             (future (event-store/try-append! s 0 (event n)))))
                     (mapv deref))]
    (is (= 1 (count (filter true? results)))
        "exactly one writer creates event 0")
    (is (= (dec writers) (count (filter false? results))))
    (is (= 0 (memory/latest-event-number s)))))

(deftest replaying-reads-every-event-in-order
  (let [s (memory/store)]
    (dotimes [n 5]
      (event-store/try-append! s n (event n)))
    (is (= (mapv event (range 5))
           (into [] (event-store/events s 0))))
    (testing "a replay can start anywhere"
      (is (= (mapv event (range 3 5))
             (into [] (event-store/events s 3)))))
    (testing "it stops at the first number that does not exist"
      (is (= [] (into [] (event-store/events s 5)))))
    (testing "reduced stops early"
      (is (= (mapv event (range 2))
             (reduce (fn [acc e]
                       (if (= 2 (count acc)) (reduced acc) (conj acc e)))
                     []
                     (event-store/events s 0)))))))

(deftest an-empty-stream-replays-to-init
  (is (= :nothing (reduce conj :nothing (event-store/events (memory/store) 0)))))

(deftest a-store-decides-for-itself-how-to-read
  ;; There is no optional acceleration protocol to satisfy. `events` returns a
  ;; reducible, and what it does inside is the store's business: one request,
  ;; a thousand, or a map lookup.
  (let [called (atom 0)
        bulk (reify event-store/EventSource
               (events [_ from]
                 (util/reducible
                  (fn [rf init]
                    (swap! called inc)
                    (reduce rf init (map event (range from 2)))))))]
    (is (= (mapv event (range 2)) (into [] (event-store/events bulk 0))))
    (is (= 1 @called) "one call, however many events came back")))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.event-store.memory-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
