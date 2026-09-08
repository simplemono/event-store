(ns simplemono.event-store.tigris.replay-failure-test
  (:require [clojure.test :refer [deftest is]]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory-client :as memory-client]
            [simplemono.event-store.tigris :as tigris])
  (:import (java.io FilterInputStream IOException)))

(defn- caught [f]
  (try (f) (catch Throwable t t)))

(defn- tracked-stream [in closed before-read]
  (proxy [FilterInputStream] [in]
    (read
      ([] (before-read) (proxy-super read))
      ([buffer] (before-read) (proxy-super read buffer))
      ([buffer offset length] (before-read) (proxy-super read buffer offset length)))
    (close [] (swap! closed inc) (proxy-super close))))

(defn- store [opened closed before-read]
  (let [objects (atom (sorted-map))
        s (tigris/store
           {:bucket "events" :prefix "replay-failure-test"
            :client (memory-client/client objects)
            :on-retry (fn [_] (throw (AssertionError. "A stream or reducer failure was retried")))
            :bundle-request (fn [_ keys]
                              (swap! opened inc)
                              (tracked-stream (memory-client/tar objects keys) closed before-read))})]
    (doseq [n (range 4)] (event-store/try-append! s n n))
    s))

(deftest a-mid-stream-failure-leaves-recovery-to-the-caller
  (let [failure (IOException. "Connection reset while reading archive")
        broken? (atom false)
        opened (atom 0) closed (atom 0)
        s (store opened closed #(when @broken? (throw failure)))
        projection (atom {:cursor -1 :events []})
        project! (fn [acc value]
                   ;; Projection and cursor are committed together by the caller.
                   (swap! projection #(-> % (assoc :cursor value) (update :events conj value)))
                   (when (= value 1) (reset! broken? true))
                   acc)]
    (is (identical? failure (caught #(reduce project! nil (event-store/events s 0)))))
    (is (= {:cursor 1 :events [0 1]} @projection))
    (is (= 2 @opened) "one probe and one batch; no automatic resume")
    (is (= @opened @closed) "both archives were closed before the exception escaped")

    (reset! broken? false)
    (reduce project! nil (event-store/events s (inc (:cursor @projection))))
    (is (= {:cursor 3 :events [0 1 2 3]} @projection)
        "explicit recovery from the committed cursor delivers no duplicates")
    (is (= @opened @closed))))

(deftest reducer-exceptions-are-not-mistaken-for-transport-failures
  (doseq [failure [(IOException. "Side effect failed")
                   (ex-info "Reducer unavailable" {:error :unavailable})
                   (IllegalStateException. "Projection failed")]
          fail-at [0 1]]
    (let [opened (atom 0) closed (atom 0)
          s (store opened closed (constantly nil))
          delivered (atom [])
          t (caught #(reduce (fn [acc value]
                               (swap! delivered conj value)
                               (when (= value fail-at) (throw failure))
                               acc)
                             nil (event-store/events s 0)))]
      (is (identical? failure t))
      (is (= (vec (range (inc fail-at))) @delivered))
      (is (= (inc fail-at) @opened))
      (is (= @opened @closed)))))

(deftest even-an-empty-delivered-prefix-does-not-reopen-the-archive
  (let [failure (IOException. "Connection reset before the first tar header")
        opened (atom 0) closed (atom 0)
        s (store opened closed #(throw failure))
        delivered (atom [])]
    (is (identical? failure
                    (caught #(reduce (fn [_ value] (swap! delivered conj value))
                                     nil (event-store/events s 0)))))
    (is (empty? @delivered))
    (is (= 1 @opened))
    (is (= 1 @closed))))
