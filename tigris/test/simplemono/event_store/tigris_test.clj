(ns simplemono.event-store.tigris-test
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory-client :as memory-client]
            [simplemono.event-store.tigris :as tigris])
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
            :bundle-size 4}
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
    (is (= [4 4 4] @requests)
        "nine events in batches of four: two full, then a short one that ends it")))

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

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.event-store.tigris-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
