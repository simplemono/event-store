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
            :bundle-request (fn [_store keys] (memory-client/tar objects keys))}
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
    (is (= [(event 0) (event 1)] (into [] (event-store/events s 0))))
    (is (= [(event 1)] (into [] (event-store/events s 1))))
    (is (= [] (into [] (event-store/events s 2))))))

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
                           :bundle-request (fn [_ keys] (memory-client/tar objs keys))
                           :on-retry #(swap! retries conj (:op %))})]
      (is (true? (event-store/try-append! s 0 (event 0))))
      (is (= [:put :put] @retries) "the caller sees no failure, only the result")
      (is (= (event 0) (reduce (fn [_ e] (reduced e)) nil (event-store/events s 0))))))

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
                             :bundle-request (fn [_ keys] (memory-client/tar objs keys))
                             :on-retry (constantly nil)})]
        (is (false? (event-store/try-append! s 0 (event 0))))
        (is (= (event 99) (reduce (fn [_ e] (reduced e)) nil (event-store/events s 0))))))))

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

(deftest a-replay-opens-with-one-key-then-reads-the-rest-bounded
  (let [objects (objects)
        requests (atom [])
        s (store objects {:bundle-request (fn [_store keys]
                                            (swap! requests conj (count keys))
                                            (memory-client/tar objects keys))})]
    (append-range! s 0 9)
    (reset! requests [])
    (is (= (mapv event (range 9)) (into [] (event-store/events s 0)))
        "every event comes back, in order, decoded from the tar")
    (is (= [1 8 1] @requests)
        "one key to see whether there is anything at all, then everything up to
         the head in one batch, then the one key past it that ends the replay
         and would catch a stream that grew while it was read. No request ever
         asks about a key that might not be there except through the leader,
         where a miss costs 9ms instead of 250.")))

(defn- counting-client
  "Wraps a client and counts what a replay is not supposed to spend any more:
   a LIST for the head, and a single-object GET."
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
  "A store that reports what each replay cost: {:lists :gets :bundles}.

   Lists and gets should both stay at zero. A replay walks the stream with
   bundle requests alone."
  [objects overrides]
  (let [lists (atom 0) gets (atom 0) bundles (atom [])
        s (store objects
                 (merge {:client (counting-client (memory-client/client objects)
                                                  lists gets)
                         :bundle-request (fn [_store keys]
                                           (swap! bundles conj (count keys))
                                           (memory-client/tar objects keys))}
                        overrides))]
    [s (fn cost
         ([] (reset! lists 0) (reset! gets 0) (reset! bundles []) nil)
         ([_] {:lists @lists :gets @gets :bundles @bundles}))]))

(deftest an-idle-replay-costs-one-request
  ;; A projection asking whether anything happened, when nothing has. This is
  ;; the call that runs most often, so it is the one worth making cheap.
  (let [objects (objects)
        [s cost] (counting-store objects {})]
    (append-range! s 0 3)
    (cost)
    (is (= [] (into [] (event-store/events s 3))))
    (is (= {:lists 0 :gets 0 :bundles [1]} (cost :report))
        "one request for one key, and no LIST anywhere")))

(deftest a-replay-spends-one-list-and-only-once-it-has-work
  ;; A LIST is Class A, about ten times a read, so a replay buys exactly one
  ;; and only after it knows there is something to read. Bounding the batches
  ;; with it is what keeps every later read off a key that is not there, which
  ;; a relaxed read discovers at about 250ms a time.
  (let [objects (objects)
        [s cost] (counting-store objects {})]
    (append-range! s 0 20)
    (cost)
    (is (= 20 (count (into [] (event-store/events s 0)))))
    (let [{:keys [lists gets]} (cost :report)]
      (is (= 1 lists) "one, for the head that bounds the batches")
      (is (zero? gets) "and no single-object read: everything goes by bundle"))))

(deftest one-event-costs-one-request
  ;; Because a batch starts at one, reading a single event by number needs no
  ;; separate point-read method on the protocol.
  (let [objects (objects)
        [s cost] (counting-store objects {})]
    (append-range! s 0 20)
    (cost)
    (is (= (event 7) (reduce (fn [_ e] (reduced e)) nil (event-store/events s 7))))
    (is (= {:lists 0 :gets 0 :bundles [1]} (cost :report)))))

(deftest a-replay-can-start-anywhere-and-stop-early
  (let [objects (objects)
        s (store objects)]
    (append-range! s 0 9)
    (is (= (mapv event (range 5 9))
           (into [] (event-store/events s 5))))
    (is (= [] (into [] (event-store/events s 9))))
    (testing "reduced stops the replay"
      (is (= (mapv event (range 2))
             (reduce (fn [acc e]
                       (if (= 2 (count acc)) (reduced acc) (conj acc e)))
                     []
                     (event-store/events s 0)))))))

(deftest an-empty-stream-replays-to-init
  (is (= :nothing (reduce conj :nothing (event-store/events (store (objects)) 0)))))

(deftest a-hole-inside-a-batch-is-reported
  (let [objects (objects)
        s (store objects)]
    (append-range! s 0 8)
    ;; Event 5 vanishes, and the batch that covers it also covers events after
    ;; it. Their names arrive where 5's was expected, which is the only way a
    ;; gap-free stream can be caught missing one.
    (swap! objects dissoc (str "org/acme/events/" (- Long/MAX_VALUE 5)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Bundle returned an unexpected object"
                          (into [] (event-store/events s 0))))))

(deftest a-hole-the-head-promised-is-not-mistaken-for-the-end
  ;; A batch is bounded by a consistently read head, so every key in it was
  ;; promised to exist. A hole on the batch's last key is the one the entry
  ;; names cannot catch, because nothing arrives after it to give it away.
  ;; Asking the leader directly is the last innocent explanation, and once that
  ;; comes back short too the events are simply gone.
  ;;
  ;; Ending the replay there would return a hundred events and lose the twenty
  ;; after the hole, looking exactly like a correct short read.
  (let [objects (objects)
        s (store objects)]
    (append-range! s 0 120)
    (swap! objects dissoc (str "org/acme/events/" (- Long/MAX_VALUE 100)))
    (let [t (try (count (into [] (event-store/events s 0)))
                 (catch clojure.lang.ExceptionInfo t t))]
      (is (instance? clojure.lang.ExceptionInfo t)
          "the replay fails rather than returning a hundred events")
      (is (= :missing-event (:error (ex-data t))))
      (is (= [99 100] [(:got (ex-data t)) (:expected (ex-data t))])
          "and says how much of the batch arrived against what was asked"))))

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
    (is (= (mapv event (range 4)) (into [] (event-store/events s 0))))
    (is (= [true false true] @seen)
        "one key through the leader to see whether anything is there, then the
         batch that carries weight relaxed, because the leader costs about five
         times the latency on a bundle and buys nothing an object store that
         never updates an object can give back. Single keys go through the
         leader either way: a key that is not there costs about 250ms to
         discover relaxed and 9ms through the leader.")))

(deftest a-short-batch-is-read-again-through-the-leader
  ;; A short batch is normally the end of the stream, and that is how a replay
  ;; finds it. But it can also be a replica that has not caught up, and
  ;; stopping there would silently truncate the replay. The re-read tells the
  ;; two apart: if the leader has more, keep going.
  (let [objects (objects)
        seen (atom [])
        stale? (atom true)
        s (store objects
                 {:bundle-request
                  (fn [store keys]
                    (swap! seen conj (consistent? store))
                    (if (and @stale? (not (consistent? store)) (< 1 (count keys)))
                      (memory-client/tar objects (butlast keys))
                      (memory-client/tar objects keys)))})]
    (append-range! s 0 4)
    (is (= (mapv event (range 4)) (into [] (event-store/events s 0)))
        "every event still arrives")
    (is (some true? @seen)
        "a short cheap read was checked against the leader before believing it")

    (testing "and a stream that really has ended just ends"
      (reset! seen [])
      (reset! stale? false)
      (is (= [] (into [] (event-store/events s 9))))
      (is (= [true] @seen)
          "one key, asked of the leader, and the answer is final"))))

(deftest f-stopping-early-is-not-mistaken-for-the-end
  (let [objects (objects)
        seen (atom [])
        s (store objects {:bundle-request (fn [store keys]
                                            (swap! seen conj (consistent? store))
                                            (memory-client/tar objects keys))})]
    (append-range! s 0 8)
    (is (= (mapv event (range 2))
           (reduce (fn [acc e] (if (= 2 (count acc)) (reduced acc) (conj acc e)))
                   []
                   (event-store/events s 0))))
    (is (= [true false] @seen)
        "the one-key batch, then a relaxed batch of two that `f` ended. Ending
         it is ordinary, so nothing is checked with the leader.")))

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
             (into [] (event-store/events s 0)))
          (str "keys of " key-length " characters"))
      (is (= (event 1) (reduce (fn [_ e] (reduced e)) nil (event-store/events s 1)))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.event-store.tigris-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
