(ns simplemono.event-store.tigris.values-test
  (:require [clojure.test :refer [deftest is testing]]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory-client :as memory-client]
            [simplemono.event-store.tigris :as tigris]
            [simplemono.event-store.tigris.codec :as codec])
  (:import (java.io FilterInputStream)))

(defn- objects
  []
  (atom (sorted-map)))

(defn- store
  ([objects] (store objects {}))
  ([objects overrides]
   (tigris/store
    (merge {:client (memory-client/client objects)
            :bucket "events"
            :prefix "org/acme"
            :bundle-request (fn [_store keys] (memory-client/tar objects keys))}
           overrides))))

(defn- caught
  [f]
  (try (f) (catch Throwable t t)))

(deftest a-put-is-create-only-and-lands-under-the-prefix
  (let [objects (objects)
        s (store objects)]
    (is (true? (tigris/put-value! s "a" {:amount 1.5M})))
    (is (false? (tigris/put-value! s "a" {:amount 1.5M}))
        "the key is taken, and an equal value does not make it ours")
    (is (= {:amount 1.5M} (codec/decode (get @objects "org/acme/a")))
        "the same codec as events, so a value survives the round trip unchanged")
    (is (= ["org/acme/a"] (keys @objects)))
    (is (true? (tigris/put-value! (store objects {:prefix ""}) "root" :r)))
    (is (contains? @objects "root") "a blank prefix puts values at the bucket root")))

(deftest unsupported-values-never-reach-put
  (let [objects (objects)
        s (store objects)]
    (is (= :incorrect (:error (ex-data (caught #(tigris/put-value! s "obj" (Object.)))))))
    (is (empty? @objects))))

(deftest values-come-back-in-order-as-entries-in-batches-of-a-hundred
  (let [objects (objects)
        requests (atom [])
        s (store objects {:bundle-request (fn [_store keys]
                                            (swap! requests conj (count keys))
                                            (memory-client/tar objects keys))})
        ks (mapv #(str "v" %) (range 250))]
    (doseq [k ks]
      (tigris/put-value! s k {:key k}))
    (tigris/put-value! s "nil" nil)
    (tigris/put-value! s "false" false)
    (let [asked (into ["false" "nil"] (rseq ks))
          entries (into [] (tigris/values s asked))]
      (is (every? map-entry? entries))
      (is (= asked (mapv key entries)) "the order asked for, not the store's")
      (is (= [false nil] (mapv val (take 2 entries)))
          "stored nil and false are values, not holes")
      (is (= (map #(hash-map :key %) (rseq ks)) (map val (drop 2 entries))))
      (is (= [100 100 52] @requests)))
    (reset! requests [])
    (is (= :init (reduce (fn [_ _] :never) :init (tigris/values s [])))
        "nothing asked for reduces to init")
    (is (empty? @requests) "and asks nothing")))

(deftest reduced-stops-reading-and-requesting
  (let [objects (objects)
        requests (atom 0)
        closed (atom 0)
        s (store objects {:bundle-request
                          (fn [_store keys]
                            (swap! requests inc)
                            (proxy [FilterInputStream] [(memory-client/tar objects keys)]
                              (close []
                                (swap! closed inc)
                                (proxy-super close))))})
        ks (mapv #(str "v" %) (range 150))]
    (doseq [k ks]
      (tigris/put-value! s k k))
    (is (= ["v0" "v1" "v2"]
           (into [] (comp (map key) (take 3)) (tigris/values s ks))))
    (is (= 1 @requests) "the second batch was never asked for")
    (is (= 1 @closed) "and the archive was closed before the call returned")))

(defn- tracing-store
  [objects requests]
  (store objects {:bundle-request
                  (fn [store keys]
                    (swap! requests conj [(count keys)
                                          (contains? (:headers store) "X-Tigris-Consistent")])
                    (memory-client/tar objects keys))}))

(deftest a-missing-value-is-reported-not-skipped
  (let [objects (objects)
        requests (atom [])
        s (tracing-store objects requests)]
    (doseq [k ["a" "b" "c"]]
      (tigris/put-value! s k k))

    (testing "a hole in the middle is given away by the object arriving in its place"
      (let [t (caught #(into [] (tigris/values s ["a" "missing" "c"])))]
        (is (= {:error :missing-value :expected "org/acme/missing" :got "org/acme/c"}
               (ex-data t)))))

    (testing "a hole at the end is asked of the leader, then reported by key"
      (reset! requests [])
      (let [delivered (atom [])
            t (caught #(reduce (fn [acc entry] (swap! delivered conj (key entry)) acc)
                               nil
                               (tigris/values s ["a" "b" "gone" "gone-too"])))]
        (is (= {:error :missing-value :missing ["gone" "gone-too"]} (ex-data t)))
        (is (= ["a" "b"] @delivered) "what arrived was handed over, once")
        (is (= [[4 false] [2 true]] @requests)
            "the batch relaxed, then only the missing suffix through the leader")))

    (testing "a single key is asked of the leader straight away"
      (reset! requests [])
      (is (= {:error :missing-value :missing ["gone"]}
             (ex-data (caught #(into [] (tigris/values s ["gone"]))))))
      (is (= [[1 true]] @requests)))))

(deftest keys-are-checked-before-any-request
  (let [objects (objects)
        requests (atom 0)
        s (store objects {:bundle-request (fn [_store _keys]
                                            (swap! requests inc)
                                            (throw (AssertionError. "no request expected")))})]
    (doseq [k ["" " " "/leading" "events/0" :keyword 42 nil]]
      (is (= :incorrect (:error (ex-data (caught #(tigris/put-value! s k 1))))) (pr-str k))
      (is (= :incorrect (:error (ex-data (caught #(tigris/values s [k]))))) (pr-str k)))
    (is (= :incorrect (:error (ex-data (caught #(tigris/values s ["a" "a"])))))
        "duplicates would make the entries ambiguous")
    (is (= :incorrect (:error (ex-data (caught #(tigris/values s "a")))))
        "a key is not a sequence of keys")
    (is (empty? @objects))
    (is (zero? @requests))))

(deftest values-and-the-stream-share-a-prefix-without-colliding
  (let [objects (objects)
        s (store objects)]
    (doseq [n (range 3)]
      (is (true? (event-store/try-append! s n {:n n}))))
    (is (true? (tigris/put-value! s "!" :sorts-before-events)))
    (is (true? (tigris/put-value! s "~" :sorts-after-events)))
    (is (= 2 (event-store/latest-event-number s)) "the head still belongs to the stream")
    (is (= [{:n 0} {:n 1} {:n 2}] (into [] (event-store/events s 0))))
    (is (= {"!" :sorts-before-events "~" :sorts-after-events}
           (into {} (tigris/values s ["~" "!"]))))))
