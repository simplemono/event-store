(ns simplemono.event-store.tigris.cancellation-test
  (:require [clojure.test :refer [deftest is]]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory-client :as memory-client]
            [simplemono.event-store.tigris :as tigris]
            [simplemono.event-store.tigris.codec :as codec])
  (:import (java.io IOException InterruptedIOException)
           (java.net SocketException)
           (java.nio.channels ClosedByInterruptException)
           (java.util.concurrent CancellationException)
           (software.amazon.awssdk.core.exception AbortedException ApiCallTimeoutException SdkClientException)
           (software.amazon.awssdk.core.sync RequestBody)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model HeadObjectRequest HeadObjectResponse
                                                     ListObjectsV2Request ListObjectsV2Response
                                                     NoSuchKeyException PutObjectRequest PutObjectResponse
                                                     S3Exception)))

(defn- caught [f]
  (try (f) (catch Throwable t t)))

(defn- store [objects overrides]
  (tigris/store
   (merge {:bucket "events" :prefix "cancellation-test"
           :client (memory-client/client objects)
           :bundle-request (fn [_ keys] (memory-client/tar objects keys))
           :on-retry (fn [_] (throw (AssertionError. "Cancellation was retried")))}
          overrides)))

(defn- service-cancellation [status]
  (-> (S3Exception/builder)
      (.statusCode status)
      (.cause (CancellationException. "Request cancelled"))
      (.build)))

(defn- cancellations []
  [(InterruptedException. "Interrupted")
   (CancellationException. "Cancelled")
   (InterruptedIOException. "Interrupted I/O")
   (ClosedByInterruptException.)
   (AbortedException/create "Aborted" (SocketException. "Socket closed"))
   (SdkClientException/create "Interrupted" (InterruptedException. "Interrupted"))
   (IOException. "I/O aborted" (CancellationException. "Cancelled"))
   (service-cancellation 409)
   (service-cancellation 412)])

(deftest a-cancelled-put-preserves-the-exception-even-if-it-landed
  (doseq [failure (cancellations) landed? [false true]]
    (let [objects (atom (sorted-map))
          ^S3Client delegate (memory-client/client objects)
          attempts (atom 0)
          client (reify S3Client
                   (^PutObjectResponse putObject [_ ^PutObjectRequest request ^RequestBody body]
                     (swap! attempts inc)
                     (when landed? (.putObject delegate request body))
                     (throw failure))
                   (^HeadObjectResponse headObject [_ ^HeadObjectRequest _request]
                     (throw (AssertionError. "Cancelled PUT must not resolve ownership"))))
          s (store objects {:client client})]
      (is (identical? failure (caught #(event-store/try-append! s 0 :event))))
      (is (= 1 @attempts))
      (is (= (if landed? [:event] [])
             (into [] (event-store/events (store objects {}) 0)))
          "an exceptional append exit does not establish whether the event was written"))))

(deftest cancellations-from-other-requests-are-not-retried-or-treated-as-absence
  (doseq [op [:previous :ownership :list :bundle]
          failure [(AbortedException/create "Aborted" (IOException. "I/O stopped"))
                   (-> (NoSuchKeyException/builder)
                       (.statusCode 404)
                       (.cause (InterruptedException. "HEAD interrupted"))
                       (.build))]]
    (let [objects (atom (sorted-map))
          ^S3Client delegate (memory-client/client objects)
          requests (atom [])
          client (reify S3Client
                   (^PutObjectResponse putObject [_ ^PutObjectRequest request ^RequestBody body]
                     (swap! requests conj :put)
                     (.putObject delegate request body)
                     ;; Simulate a committed PUT whose SDK retry returned 412.
                     (throw (-> (S3Exception/builder) (.statusCode 412) (.build))))
                   (^HeadObjectResponse headObject [_ ^HeadObjectRequest _request]
                     (swap! requests conj :head)
                     (throw failure))
                   (^ListObjectsV2Response listObjectsV2 [_ ^ListObjectsV2Request _request]
                     (swap! requests conj :list)
                     (throw failure)))
          s (store objects {:client client
                            :bundle-request (fn [_ _] (swap! requests conj :bundle) (throw failure))})
          t (caught #(case op
                       :previous (event-store/try-append! s 1 :event)
                       :ownership (event-store/try-append! s 0 :event)
                       :list (event-store/latest-event-number s)
                       :bundle (into [] (event-store/events s 0))))]
      (is (identical? failure t))
      (is (= (case op :previous [:head] :ownership [:put :head] :list [:list] :bundle [:bundle])
             @requests)))))

(deftest an-already-interrupted-thread-does-not-start-a-request
  (doseq [op [:put :previous :list :bundle]]
    (let [requests (atom [])
          client (reify S3Client
                   (^PutObjectResponse putObject [_ ^PutObjectRequest _request ^RequestBody _body]
                     (swap! requests conj :put)
                     (throw (AssertionError. "Unexpected PUT")))
                   (^HeadObjectResponse headObject [_ ^HeadObjectRequest _request]
                     (swap! requests conj :head)
                     (throw (AssertionError. "Unexpected HEAD")))
                   (^ListObjectsV2Response listObjectsV2 [_ ^ListObjectsV2Request _request]
                     (swap! requests conj :list)
                     (throw (AssertionError. "Unexpected LIST"))))
          s (store (atom (sorted-map))
                   {:client client :bundle-request (fn [_ _] (swap! requests conj :bundle)
                                                     (throw (AssertionError. "Unexpected bundle")))})
          t (try
              (.interrupt (Thread/currentThread))
              (caught #(case op
                         :put (event-store/try-append! s 0 :event)
                         :previous (event-store/try-append! s 1 :event)
                         :list (event-store/latest-event-number s)
                         :bundle (into [] (event-store/events s 0))))
              (finally (Thread/interrupted)))]
      (is (instance? InterruptedException t))
      (is (empty? @requests)))))

(deftest an-interrupt-flag-prevents-retry-even-without-a-cancellation-cause
  (doseq [failure [(IOException. "Connection closed")
                   (ApiCallTimeoutException/create 100)
                   (-> (S3Exception/builder) (.statusCode 409) (.build))]]
    (let [attempts (atom 0)
          client (reify S3Client
                   (^PutObjectResponse putObject [_ ^PutObjectRequest _request ^RequestBody _body]
                     (swap! attempts inc)
                     (.interrupt (Thread/currentThread))
                     (throw failure)))
          s (store (atom (sorted-map)) {:client client})
          t (try (caught #(event-store/try-append! s 0 :event))
                 (finally (Thread/interrupted)))]
      (is (identical? failure t))
      (is (= 1 @attempts)))))

(deftest interrupting-backoff-can-leave-a-committed-append
  (with-redefs-fn
    {#'tigris/retry-delay-ms (constantly 60000)}
    (fn []
      (let [objects (atom (sorted-map))
            ^S3Client delegate (memory-client/client objects)
            attempts (atom 0)
            retry-ready (promise)
            result (promise)
            client (reify S3Client
                     (^PutObjectResponse putObject [_ ^PutObjectRequest request ^RequestBody body]
                       (when-not (= 1 (swap! attempts inc))
                         (throw (AssertionError. "PUT retried after cancellation")))
                       (.putObject delegate request body)
                       (throw (SdkClientException/create "Response lost" (IOException. "Reset")))))
            s (store objects {:client client :on-retry (fn [_] (deliver retry-ready :ready))})
            worker (Thread. ^Runnable
                            (fn [] (deliver result (caught #(event-store/try-append! s 0 :event))))
                            "event-store-cancellation-test")]
        (.setDaemon worker true)
        (try
          (.start worker)
          (is (= :ready (deref retry-ready 5000 :timed-out)))
          (.interrupt worker)
          (is (instance? InterruptedException (deref result 5000 :timed-out)))
          (.join worker 5000)
          (is (not (.isAlive worker)))
          (is (= 1 @attempts))
          (is (= [:event] (mapv codec/decode (vals @objects))))
          (finally
            (.interrupt worker)
            (.join worker 5000)))))))
