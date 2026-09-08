(ns simplemono.event-store.tigris.codec-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [clojure.walk :as walk]
            [simplemono.event-store.tigris.codec :as codec]
            [taoensso.nippy :as nippy]))

;; These values also describe the frozen compatibility fixtures. Do not rewrite
;; the fixture files when upgrading Nippy: future readers must still read them.
(def fixture-event
  (with-meta
    {:event/type :example/happened
     :event/id #uuid "92439569-0441-409d-b9ef-12a7b8f561db"
     :event/occurred-at #inst "2026-08-24T00:00:00.000-00:00"
     :instant (java.time.Instant/parse "2026-08-24T00:00:00.123456789Z")
     :text "Hello, 世界"
     :values [nil true false \λ 'example/symbol]
     :numbers [(byte -1) (short 2) (int 3) 4 (float 1.5) 2.5
               123456789012345678901N (biginteger "98765432109876543210")
               123.45M 2/3]
     :collections ['(1 :two "three") #{:a :b} {:nested [1 2 3]}
                   (sorted-map :a 1 :b 2) (sorted-set 1 2 3)]
     :bytes (byte-array [-128 -1 0 1 127])}
    {:source :compatibility-fixture}))

(def large-fixture-event
  (assoc fixture-event :payload (apply str (repeat 4096 "compressible-event-data/"))))

(defn- comparable
  "Arrays have identity equality; compare their contents in round-trip tests."
  [value]
  (walk/postwalk #(if (bytes? %) (vec %) %) value))

(defrecord ApplicationRecord [value])
(deftype ApplicationObject [value])

(deftest supported-data-round-trips
  (doseq [value [nil true false 0 :event "text" 'symbol \x
                 [] {} #{} '() (list 1 2 3) (map inc [1 2 3])
                 (byte-array 0) (byte-array [-128 0 127])
                 fixture-event large-fixture-event]]
    (let [encoded (codec/encode value)
          decoded (codec/decode encoded)]
      (is (= [78 80 89] (vec (take 3 encoded))) "regular Nippy header, not gzip or fast-freeze")
      (is (= (comparable value) (comparable decoded)))
      (is (= (meta value) (meta decoded)))
      (when (bytes? value) (is (bytes? decoded)))))
  (is (bytes? (:bytes (codec/decode (codec/encode fixture-event)))))
  (is (< (alength (codec/encode large-fixture-event)) 8192)
      "automatic compression handles a large, repetitive payload"))

(deftest codec-settings-do-not-leak-in-from-the-caller
  (binding [*print-length* 1
            *print-level* 1
            *print-dup* true
            nippy/*incl-metadata?* false
            nippy/*thaw-xform* (filter (constantly false))
            nippy/*auto-freeze-compressor* (fn [_] (throw (AssertionError. "Caller compressor used")))
            nippy/*shared-dict* (nippy/shared-dict [:event/type :example/happened])]
    (let [decoded (codec/decode (codec/encode large-fixture-event))]
      (is (= (comparable large-fixture-event) (comparable decoded)))
      (is (= (meta large-fixture-event) (meta decoded))))))

(deftest unsupported-values-are-rejected-without-fallbacks
  (doseq [value [(Object.) (atom 1) identity
                 (->ApplicationRecord 1) (ApplicationObject. 1)
                 (java.net.URI/create "https://example.com") #"pattern"
                 (java.util.HashMap.) (java.util.concurrent.atomic.AtomicLong. 1)
                 (java.sql.Timestamp/valueOf "2026-08-24 00:00:00.123456789")
                 (object-array [1]) (int-array [1])
                 (sorted-map-by > 2 :two 1 :one)]]
    (testing (str (class value))
      (binding [nippy/*freeze-fallback* :write-unfreezable
                nippy/*freeze-serializable-allowlist* #{"*"}]
        (doseq [event [value {:nested [value]} {value :map-key}
                       (with-meta {:event/type :example/happened} {:unsupported value})]]
          (let [t (try (codec/encode event) (catch clojure.lang.ExceptionInfo t t))]
            (is (instance? clojure.lang.ExceptionInfo t))
            (is (= :incorrect (:error (ex-data t))))))))))

(deftest decoding-does-not-deserialize-java-objects-or-return-placeholders
  (let [bytes (nippy/freeze (java.util.concurrent.atomic.AtomicLong. 42))]
    (binding [nippy/*thaw-serializable-allowlist* #{"*"}
              nippy/*serializable-whitelist* #{"*"}]
      (is (thrown? clojure.lang.ExceptionInfo (codec/decode bytes))))))

(deftest other-storage-formats-are-rejected
  (doseq [bytes [(byte-array 0)
                 (.getBytes "{:event/type :legacy/edn}" "UTF-8")
                 (nippy/fast-freeze {:event/type :headerless})]]
    (is (thrown? clojure.lang.ExceptionInfo (codec/decode bytes)))))

(deftest frozen-events-remain-readable
  (doseq [[file expected] [["nippy-3.9.0-small.bin" fixture-event]
                           ["nippy-3.9.0-large.bin" large-fixture-event]]]
    (testing file
      (with-open [in (io/input-stream (io/resource (str "fixtures/" file)))]
        (let [decoded (codec/decode (.readAllBytes in))]
          (is (= (comparable expected) (comparable decoded)))
          (is (= (meta expected) (meta decoded)))
          (is (bytes? (:bytes decoded))))))))
