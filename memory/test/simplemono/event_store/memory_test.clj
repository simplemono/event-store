(ns simplemono.event-store.memory-test
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory :as memory]))

(defn- event
  [n]
  {:event/occurred-at #inst "2026-08-25T00:00:00.000-00:00"
   :event/type :example/happened
   :event/n n})

(deftest appends-are-create-only-and-gap-free
  (let [s (memory/store)]
    (is (nil? (event-store/latest-event-number s)))
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
    (is (= 1 (event-store/latest-event-number s)))
    (is (= (event 0) (event-store/get-event s 0)))
    (is (= (event 1) (event-store/get-event s 1)))
    (is (nil? (event-store/get-event s 2)))))

(deftest the-stream-is-the-atom
  (let [state (atom (sorted-map))
        s (memory/store state)]
    (event-store/try-append! s 0 (event 0))
    (event-store/try-append! s 1 (event 1))
    (is (= {0 (event 0) 1 (event 1)} @state))
    (testing "a second store over the same atom sees the same stream"
      (is (= 1 (event-store/latest-event-number (memory/store state)))))))

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
    (is (= 0 (event-store/latest-event-number s)))))

(deftest replaying-reads-every-event-in-order
  (let [s (memory/store)]
    (dotimes [n 5]
      (event-store/try-append! s n (event n)))
    (is (= (mapv event (range 5))
           (event-store/reduce-events s 0 conj [])))
    (testing "a replay can start anywhere"
      (is (= (mapv event (range 3 5))
             (event-store/reduce-events s 3 conj []))))
    (testing "it stops at the first number that does not exist"
      (is (= [] (event-store/reduce-events s 5 conj []))))
    (testing "reduced stops early"
      (is (= (mapv event (range 2))
             (event-store/reduce-events s 0
                                        (fn [acc e]
                                          (if (= 2 (count acc))
                                            (reduced acc)
                                            (conj acc e)))
                                        []))))))

(deftest an-empty-stream-replays-to-init
  (is (= :nothing (event-store/reduce-events (memory/store) 0 conj :nothing))))

(deftest a-store-with-its-own-bulk-read-is-used-instead-of-the-fallback
  (let [called (atom 0)
        accelerated (reify
                      event-store/EventStore
                      (try-append! [_ _ _] true)
                      (get-event [_ _] (throw (AssertionError. "should not be reached")))
                      (latest-event-number [_] 1)

                      event-store/EventReplay
                      (-reduce-events [_ from f init _opts]
                        (swap! called inc)
                        (reduce f init (map event (range from 2)))))]
    (is (= (mapv event (range 2))
           (event-store/reduce-events accelerated 0 conj [])))
    (is (= 1 @called))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.event-store.memory-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
