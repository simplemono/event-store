(ns simplemono.event-store.s3-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is run-tests testing]]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory-client :as memory-client]
            [simplemono.event-store.s3 :as s3])
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
                                                     PutObjectResponse)))

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
   (s3/store
    (merge {:client (memory-client/client objects)
            :bucket "events"
            :prefix "org/acme"
            ;; Tests pack on the appending thread, so an append that completes
            ;; a range has finished packing when it returns.
            :pack-async? false}
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

(deftest completing-a-range-writes-a-pack
  (let [objects (objects)
        s (store objects {:pack-size 4})]
    (append-range! s 0 3)
    (is (empty? (filter #(str/starts-with? % "org/acme/packs/") (keys @objects)))
        "a partial range is not packed")
    (event-store/try-append! s 3 (event 3))
    (is (= ["org/acme/packs/4/9223372036854775807"]
           (filter #(str/starts-with? % "org/acme/packs/") (keys @objects))))
    (is (= (pr-str (mapv event (range 4)))
           (gunzip (get @objects "org/acme/packs/4/9223372036854775807"))))
    (testing "the tail beyond the pack still reads from its own object"
      (event-store/try-append! s 4 (event 4))
      (is (= (event 4) (event-store/get-event s 4))))
    (testing "the packed events are read through the pack, not their objects"
      ;; Deleting the packed event objects is only safe because this test is
      ;; finished with them; the real store never deletes an event.
      (doseq [n (range 4)]
        (swap! objects dissoc (str "org/acme/events/" (- Long/MAX_VALUE n))))
      (let [reader (store objects {:pack-size 4})]
        (is (= (mapv event (range 4))
               (mapv #(event-store/get-event reader %) (range 4))))))))

(deftest packing-catches-up-ranges-it-missed
  (let [objects (objects)
        ;; Nothing is packed while :pack? is false, so the second store starts
        ;; with several complete ranges and no packs at all.
        unpacked (store objects {:pack-size 4 :pack? false})]
    (append-range! unpacked 0 11)
    (is (empty? (filter #(str/starts-with? % "org/acme/packs/") (keys @objects))))
    (let [s (store objects {:pack-size 4})]
      (event-store/try-append! s 11 (event 11))
      (is (= ["org/acme/packs/4/9223372036854775805"
              "org/acme/packs/4/9223372036854775806"
              "org/acme/packs/4/9223372036854775807"]
             (filter #(str/starts-with? % "org/acme/packs/") (keys @objects)))
          "one boundary packs every full range below the head, oldest first"))))

(deftest a-missing-pack-only-slows-reads-down
  (let [objects (objects)
        s (store objects {:pack-size 4})]
    (append-range! s 0 8)
    (is (= 2 (count (filter #(str/starts-with? % "org/acme/packs/") (keys @objects)))))
    (swap! objects dissoc "org/acme/packs/4/9223372036854775807")
    (testing "a fresh store falls back to the individual event objects"
      (let [reader (store objects {:pack-size 4})]
        (is (= (mapv event (range 8))
               (mapv #(event-store/get-event reader %) (range 8))))))))

(deftest changing-the-pack-size-orphans-the-old-packs
  (let [objects (objects)
        small (store objects {:pack-size 4})]
    (append-range! small 0 8)
    (is (= 2 (count (filter #(str/starts-with? % "org/acme/packs/4/") (keys @objects)))))
    (testing "a store with another size packs into its own namespace"
      (let [large (store objects {:pack-size 8})]
        (append-range! large 8 16)
        (is (= ["org/acme/packs/8/9223372036854775806"
                "org/acme/packs/8/9223372036854775807"]
               (filter #(str/starts-with? % "org/acme/packs/8/") (keys @objects)))
            "packing keeps working, and re-packs from event 0 under the new size")
        (is (= 2 (count (filter #(str/starts-with? % "org/acme/packs/4/") (keys @objects))))
            "the old packs are untouched, just orphaned")
        (testing "and every event still reads correctly under either size"
          (is (= (mapv event (range 16))
                 (mapv #(event-store/get-event large %) (range 16))))
          (is (= (mapv event (range 16))
                 (mapv #(event-store/get-event (store objects {:pack-size 4}) %)
                       (range 16)))))))))

(defn- flaky-put-client
  "Delegates everything to a memory client, but every putObject throws the way
   a connection reset does — after storing the object or without storing it."
  [objects store-it?]
  (let [^S3Client delegate (memory-client/client objects)]
    (reify S3Client
      (^PutObjectResponse putObject [_ ^PutObjectRequest request ^RequestBody body]
        (when store-it?
          (.putObject delegate request body))
        (throw (SdkClientException/create "Connection reset")))

      (^ResponseInputStream getObject [_ ^GetObjectRequest request]
        (.getObject delegate request))

      (^HeadObjectResponse headObject [_ ^HeadObjectRequest request]
        (.headObject delegate request))

      (^ListObjectsV2Response listObjectsV2 [_ ^ListObjectsV2Request request]
        (.listObjectsV2 delegate request)))))

(deftest an-ambiguous-put-is-decided-by-the-stored-object
  (testing "our own event under the key means the put landed"
    (let [objs (objects)
          s (s3/store {:client (flaky-put-client objs true)
                                :bucket "events"
                                :prefix "org/acme"
                                :pack-async? false})]
      (is (true? (event-store/try-append! s 0 (event 0))))))
  (testing "somebody else's event under the key means we lost"
    (let [objs (objects)
          seeded (store objs)]
      (is (true? (event-store/try-append! seeded 0 (event 0))))
      (let [s (s3/store {:client (flaky-put-client objs false)
                                  :bucket "events"
                                  :prefix "org/acme"
                                  :pack-async? false})]
        (is (false? (event-store/try-append! s 0 (event 99)))))))
  (testing "no object at all leaves the outcome genuinely unknown"
    (let [objs (objects)
          s (s3/store {:client (flaky-put-client objs false)
                                :bucket "events"
                                :prefix "org/acme"
                                :pack-async? false})]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Append outcome ambiguous"
                            (event-store/try-append! s 0 (event 0)))))))

(deftest background-packing-completes
  (let [objects (objects)
        s (store objects {:pack-size 4 :pack-async? true})]
    (append-range! s 0 4)
    (let [deadline (+ (System/currentTimeMillis) 5000)]
      (while (and (empty? (filter #(str/starts-with? % "org/acme/packs/") (keys @objects)))
                  (< (System/currentTimeMillis) deadline))
        (Thread/sleep 10)))
    (is (= ["org/acme/packs/4/9223372036854775807"]
           (filter #(str/starts-with? % "org/acme/packs/") (keys @objects))))))

(deftest a-blank-prefix-puts-events-at-the-bucket-root
  (let [objects (objects)
        s (store objects {:prefix ""})]
    (is (true? (event-store/try-append! s 0 (event 0))))
    (is (= ["events/9223372036854775807"] (keys @objects)))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'simplemono.event-store.s3-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
