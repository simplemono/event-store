(ns simplemono.event-store.tigris-test
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory-client :as memory-client]
            [simplemono.event-store.tigris :as tigris]
            [simplemono.event-store.tigris.bundle :as bundle])
  (:import (java.io ByteArrayInputStream)
           (java.util.zip GZIPInputStream)
           (software.amazon.awssdk.core ResponseInputStream)
           (software.amazon.awssdk.core.exception SdkClientException)
           (software.amazon.awssdk.core.sync RequestBody)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model GetObjectRequest
                                                     HeadObjectRequest
                                                     HeadObjectResponse
                                                     ListObjectsV2Request
                                                     ListObjectsV2Response
                                                     PutObjectRequest
                                                     PutObjectResponse
                                                     S3Exception)))

(defn- event
  [n]
  {:event/number n
   :event/id (java.util.UUID/nameUUIDFromBytes (.getBytes (str n)))
   :event/appended-at #inst "2026-08-24T00:00:00.000-00:00"
   :event/occurred-at #inst "2026-08-24T00:00:00.000-00:00"
   :event/type :example/happened
   :event/subjects [(str "/example/" n "/")]})

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
            :bundle-request (fn [_store keys] (memory-client/tar objects keys))
            :bundle-size 4
            ;; These streams are a handful of events long, so the default
            ;; probe limit would read them one at a time and never reach a
            ;; bundle. The bundle tests want the bundle path; the probe tests
            ;; below set their own limit.
            :probe-limit 0}
           overrides))))

(defn- gunzip
  [bytes]
  (with-open [gzip (GZIPInputStream. (ByteArrayInputStream. bytes))]
    (slurp gzip :encoding "UTF-8")))

(defn- append-range!
  [s from to]
  (doseq [n (range from to)]
    (is (true? (event-store/try-append! s n (event n)))
        (str "appended event " n))))

(deftest appends-are-create-only-and-gap-free
  (let [objects (objects)
        s (store objects)]
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

(deftest objects-use-inverted-gzip-edn-keys
  (let [objects (objects)
        s (store objects)]
    (append-range! s 0 2)
    (is (= ["org/acme/events/9223372036854775806"
            "org/acme/events/9223372036854775807"]
           (keys @objects))
        "the newest event sorts first, so the head is one maxKeys=1 LIST")
    (is (= (pr-str (event 0))
           (gunzip (get @objects "org/acme/events/9223372036854775807"))))))

(defn- failing-put-client
  "Delegates to a memory client, but the first `failures` putObject calls throw
   the way a connection reset does — after storing the object or without
   storing it, so both sides of an uncertain write can be exercised."
  [objects failures store-it?]
  (let [^S3Client delegate (memory-client/client objects)
        remaining (atom failures)]
    (reify S3Client
      (^PutObjectResponse putObject [_ ^PutObjectRequest request ^RequestBody body]
        (if (pos? @remaining)
          (do
            (swap! remaining dec)
            (when store-it?
              (.putObject delegate request body))
            (throw (SdkClientException/create "Connection reset")))
          (.putObject delegate request body)))

      (^ResponseInputStream getObject [_ ^GetObjectRequest request]
        (.getObject delegate request))

      (^HeadObjectResponse headObject [_ ^HeadObjectRequest request]
        (.headObject delegate request))

      (^ListObjectsV2Response listObjectsV2 [_ ^ListObjectsV2Request request]
        (.listObjectsV2 delegate request)))))

(deftest transient-failures-are-retried-until-the-store-answers
  (testing "a put that fails twice and then succeeds still appends"
    (let [objs (objects)
          retries (atom [])
          s (tigris/store {:client (failing-put-client objs 2 false)
                       :bucket "events"
                       :prefix "org/acme"
                       :on-retry #(swap! retries conj (:op %))})]
      (is (true? (event-store/try-append! s 0 (event 0))))
      (is (= [:put :put] @retries) "the caller sees no failure, only the result")
      (is (= (event 0) (event-store/get-event s 0)))))

  (testing "a put that landed before it failed is recognised as ours"
    (let [objs (objects)
          s (tigris/store {:client (failing-put-client objs 1 true)
                       :bucket "events"
                       :prefix "org/acme"
                       :on-retry (constantly nil)})]
      ;; The first attempt stores the object and then throws, so the retry
      ;; finds the key taken. Reading it back shows the write was ours.
      (is (true? (event-store/try-append! s 0 (event 0))))))

  (testing "a key taken by somebody else is still a lost race"
    (let [objs (objects)
          winner (store objs)]
      (is (true? (event-store/try-append! winner 0 (event 99))))
      (let [s (tigris/store {:client (failing-put-client objs 1 false)
                         :bucket "events"
                         :prefix "org/acme"
                         :on-retry (constantly nil)})]
        (is (false? (event-store/try-append! s 0 (event 0))))
        (is (= (event 99) (event-store/get-event s 0)))))))

(deftest a-terminal-failure-is-thrown-at-once
  (let [attempts (atom 0)
        forbidden (-> (S3Exception/builder) (.statusCode 403) (.message "Forbidden") (.build))
        client (reify S3Client
                 (^PutObjectResponse putObject [_ ^PutObjectRequest _request ^RequestBody _body]
                   (swap! attempts inc)
                   (throw forbidden)))
        s (tigris/store {:client client
                     :bucket "events"
                     :prefix "org/acme"
                     :on-retry (constantly nil)})]
    (is (thrown? S3Exception (event-store/try-append! s 0 (event 0))))
    (is (= 1 @attempts)
        "a bad key or a missing bucket must fail loudly, not retry forever")))

(deftest a-blank-prefix-puts-events-at-the-bucket-root
  (let [objects (objects)
        s (store objects {:prefix ""})]
    (is (true? (event-store/try-append! s 0 (event 0))))
    (is (= ["events/9223372036854775807"] (keys @objects)))))

(deftest replaying-fetches-events-in-bundles
  (let [objects (objects)
        requests (atom [])
        s (store objects {:bundle-request (fn [_store keys]
                                            (swap! requests conj (count keys))
                                            (memory-client/tar objects keys))})]
    (append-range! s 0 9)
    (reset! requests [])
    (is (= (mapv event (range 9))
           (event-store/reduce-events s 0 conj []))
        "every event comes back, in order, decoded from the tar")
    (is (= [4 4 1] @requests)
        "nine events in batches of four, the last batch bounded by the head")))

(deftest a-replay-can-start-anywhere-and-stop-early
  (let [objects (objects)
        s (store objects)]
    (append-range! s 0 9)
    (is (= (mapv event (range 5 9))
           (event-store/reduce-events s 5 conj [])))
    (is (= [] (event-store/reduce-events s 9 conj [])))
    (testing "reduced stops the replay"
      (is (= (mapv event (range 2))
             (event-store/reduce-events s 0
                                        (fn [acc e]
                                          (if (= 2 (count acc))
                                            (reduced acc)
                                            (conj acc e)))
                                        []))))))

(deftest an-empty-stream-replays-to-init
  (is (= :nothing (event-store/reduce-events (store (objects)) 0 conj :nothing))))

(deftest a-bundle-that-skips-an-event-is-reported
  (let [objects (objects)
        s (store objects)]
    (append-range! s 0 8)
    ;; A gap-free stream cannot legitimately be missing an event, so a bundle
    ;; that silently leaves one out must stop the replay rather than drop it.
    (swap! objects dissoc "org/acme/events/9223372036854775805")
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Bundle returned an unexpected object"
                          (event-store/reduce-events s 0 conj [])))))

(defn- counting-client
  "Wraps a client and counts the two requests a replay can spend: the LIST that
   finds the head, and the GET that reads one event."
  [client lists gets]
  (proxy [software.amazon.awssdk.services.s3.S3Client] []
    (listObjectsV2 [request]
      (swap! lists inc)
      (.listObjectsV2 client request))
    (getObject [& args]
      (swap! gets inc)
      (clojure.lang.Reflector/invokeInstanceMethod client "getObject"
                                                   (into-array Object args)))
    (headObject [request] (.headObject client request))
    (putObject [request body] (.putObject client request body))
    (close [] (.close client))
    (serviceName [] (.serviceName client))))

(defn- counting-store
  "A store reporting what each replay cost: {:lists :gets :bundles}."
  [objects overrides]
  (let [lists (atom 0) gets (atom 0) bundles (atom [])
        s (store objects
                 (merge {:client (counting-client (memory-client/client objects)
                                                  lists gets)
                         :bundle-request (fn [_store keys]
                                           (swap! bundles conj (count keys))
                                           (memory-client/tar objects keys))}
                        overrides))]
    [s (fn reset-and-report
         ([] (reset! lists 0) (reset! gets 0) (reset! bundles []) nil)
         ([_] {:lists @lists :gets @gets :bundles @bundles}))]))

(deftest an-idle-replay-costs-one-get-and-no-list
  ;; The hot path: a projection asking whether anything happened, when nothing
  ;; has. One GET is Class B; a LIST is Class A and about ten times the price.
  (let [objects (objects)
        [s cost] (counting-store objects {:probe-limit 4})]
    (append-range! s 0 3)
    (cost)
    (is (= [] (event-store/reduce-events s 3 conj [])))
    (is (= {:lists 0 :gets 1 :bundles []} (cost :report))
        "the cheapest question is whether the next event exists")))

(deftest a-short-replay-stays-on-single-reads
  (let [objects (objects)
        [s cost] (counting-store objects {:probe-limit 4})]
    (append-range! s 0 3)
    (cost)
    (is (= (mapv event (range 1 3)) (event-store/reduce-events s 1 conj [])))
    (is (= {:lists 0 :gets 3 :bundles []} (cost :report))
        "two events and the miss that ends it, with no LIST")))

(deftest a-long-replay-escalates-to-the-head-and-bundles
  (let [objects (objects)
        [s cost] (counting-store objects {:probe-limit 4 :bundle-size 4})]
    (append-range! s 0 12)
    (cost)
    (is (= (mapv event (range 12)) (event-store/reduce-events s 0 conj [])))
    (let [{:keys [lists gets bundles]} (cost :report)]
      (is (= 1 lists) "one LIST, amortised over everything left")
      (is (= [4 4] bundles) "the rest in bounded batches")
      (is (= 5 gets)
          "four single reads to reach the limit, and one past the end: the
           stream may have grown while we read it, and asking costs a GET
           rather than the second LIST this used to spend"))))

(deftest known-head-replays-without-a-list-at-all
  ;; Nothing is ever deleted, so an event number the caller has seen before is
  ;; still there. A projection replaying up to its own SQLite cursor therefore
  ;; needs no discovery whatsoever.
  (let [objects (objects)
        [s cost] (counting-store objects {:probe-limit 4 :bundle-size 4})]
    (append-range! s 0 8)
    (cost)
    (is (= (mapv event (range 8))
           (event-store/reduce-events s 0 conj [] {:known-head 7})))
    (is (= {:lists 0 :gets 1 :bundles [4 4]} (cost :report))
        "eight known events in two bounded batches and no LIST at all; the one
         GET is the look past the bound, since a floor cannot rule out growth")))

(deftest known-head-still-finds-what-was-appended-after-it
  (let [objects (objects)
        [s cost] (counting-store objects {:probe-limit 4 :bundle-size 4})]
    (append-range! s 0 6)
    (cost)
    (is (= (mapv event (range 6))
           (event-store/reduce-events s 0 conj [] {:known-head 3}))
        "the bound is a floor, not a ceiling")
    (let [{:keys [lists bundles]} (cost :report)]
      (is (= [4] bundles) "the known range goes in one batch")
      (is (zero? lists) "and the two after it fit inside the probe limit"))))

(deftest known-head-that-over-promises-is-not-mistaken-for-the-end
  (let [objects (objects)
        [s _] (counting-store objects {:probe-limit 4 :bundle-size 8})]
    (append-range! s 0 3)
    ;; Claiming events the stream does not hold must fail loudly. Returning the
    ;; three that exist would look exactly like a correct short replay.
    (let [t (try (event-store/reduce-events s 0 conj [] {:known-head 20})
                 nil
                 (catch clojure.lang.ExceptionInfo t t))]
      (is (some? t) "an over-promised bound throws")
      (is (re-find #"does not hold the events" (ex-message t)))
      (is (= 20 (:known-head (ex-data t)))
          "and names the promise, so the cause is not a guess"))))

(deftest known-head-is-validated
  (let [s (store (objects))]
    (doseq [bad [-1 1.5 "3"]]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"non-negative integer"
                            (event-store/reduce-events s 0 conj [] {:known-head bad}))
          (str "rejects " (pr-str bad))))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"promises nothing"
                          (event-store/reduce-events s 10 conj [] {:known-head 3}))
        "a bound below the range being replayed is a caller mistake")))

(deftest headers-the-jdk-owns-are-not-copied-onto-the-request
  ;; The signer returns Host because it is signed, and the JDK's HttpClient
  ;; throws IllegalArgumentException rather than let a caller set it. The JDK
  ;; derives an identical Host from the same URI, so dropping it is safe --
  ;; but forgetting to drop it breaks every bundle request, and no test that
  ;; stubs :bundle-request can see that.
  (let [signed {"Host" ["events.t3.storage.dev"]
                "Authorization" ["AWS4-HMAC-SHA256 ..."]
                "x-amz-date" ["20260826T000000Z"]
                "Content-Length" ["123"]}
        extra {"X-Tigris-Consistent" "true"
               "x-tigris-bundle-format" "tar"}
        headers (into {} (bundle/settable-headers signed extra))]
    (is (nil? (get headers "Host")))
    (is (nil? (get headers "Content-Length")))
    (is (= "AWS4-HMAC-SHA256 ..." (get headers "Authorization")))
    (is (= "20260826T000000Z" (get headers "x-amz-date")))
    (is (= "true" (get headers "X-Tigris-Consistent")))
    (is (= "tar" (get headers "x-tigris-bundle-format")))))

(defn- consistent?
  [store]
  (= "true" (get (:headers store) "X-Tigris-Consistent")))

(deftest the-bundle-does-not-pay-for-the-leader
  (let [objects (objects)
        seen (atom [])
        s (store objects {:bundle-request (fn [store keys]
                                            (swap! seen conj (consistent? store))
                                            (memory-client/tar objects keys))})]
    (append-range! s 0 4)
    (event-store/reduce-events s 0 conj [])
    (is (= [false] @seen)
        "reading through the leader costs about five times the latency on a
         bundle, and buys nothing an object store that never updates an object
         can give back")))

(deftest a-short-batch-is-read-again-through-the-leader
  ;; The head promised four events. A replica that has not caught up returns
  ;; three, and returning those would silently truncate the replay -- the
  ;; caller would rebuild a read model missing an event and never know.
  (let [objects (objects)
        seen (atom [])
        stale? (atom true)
        s (store objects
                 {:bundle-request
                  (fn [store keys]
                    (swap! seen conj (consistent? store))
                    (if (and @stale? (not (consistent? store)))
                      (memory-client/tar objects (butlast keys))
                      (memory-client/tar objects keys)))})]
    (append-range! s 0 4)
    (is (= (mapv event (range 4))
           (event-store/reduce-events s 0 conj []))
        "every event still arrives")
    (is (= [false true] @seen)
        "the cheap read came up short, so the batch was read again consistently")

    (testing "and a batch that is short even through the leader fails loudly"
      (reset! seen [])
      (reset! stale? false)
      (let [always-short (store objects
                                {:bundle-request
                                 (fn [_store keys]
                                   (memory-client/tar objects (butlast keys)))})]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"fewer events than the stream holds"
             (event-store/reduce-events always-short 0 conj [])))))))

(deftest f-stopping-early-is-not-mistaken-for-a-short-batch
  (let [objects (objects)
        seen (atom [])
        s (store objects {:bundle-request (fn [store keys]
                                            (swap! seen conj (consistent? store))
                                            (memory-client/tar objects keys))})]
    (append-range! s 0 8)
    (is (= (mapv event (range 2))
           (event-store/reduce-events s 0
                                      (fn [acc e]
                                        (if (= 2 (count acc)) (reduced acc) (conj acc e)))
                                      [])))
    (is (= [false] @seen)
        "`f` ending it is ordinary, so there is nothing to re-read")))

(deftest keys-too-long-for-ustar-still-replay
  ;; Tigris picks the tar dialect by key length: a key that fits is a plain
  ;; ustar entry, a longer one is split into the ustar name prefix, and past
  ;; the 256 characters ustar can hold it becomes a POSIX pax extended header.
  ;; A hand-written reader that only knew ustar passed every test here and
  ;; threw against the real service, which is why the reader is a library now.
  ;;
  ;; The double is not a perfect mimic: writing in POSIX long-file mode it
  ;; reaches for pax at 100 characters, where Tigris keeps splitting into the
  ;; ustar prefix until 256. So this covers plain ustar and pax, and Tigris's
  ;; middle case is covered by the check against the real bucket.
  (doseq [prefix-length [40 140 240 300]]
    (let [objects (objects)
          prefix (subs (apply str (repeat 20 "0123456789abcdef/")) 0 prefix-length)
          s (store objects {:prefix prefix})
          key-length (+ prefix-length (count "/events/") 19)]
      (append-range! s 0 3)
      (is (= (mapv event (range 3))
             (event-store/reduce-events s 0 conj []))
          (str "keys of " key-length " characters"))
      (is (= (event 1) (event-store/get-event s 1))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.event-store.tigris-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
