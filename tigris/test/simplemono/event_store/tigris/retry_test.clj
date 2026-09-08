(ns simplemono.event-store.tigris.retry-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.memory-client :as memory-client]
            [simplemono.event-store.tigris :as tigris]
            [simplemono.event-store.tigris.bundle :as bundle])
  (:import (java.io ByteArrayInputStream FileNotFoundException IOException)
           (java.net SocketException SocketTimeoutException)
           (java.net.http HttpClient HttpResponse HttpTimeoutException)
           (javax.net.ssl SSLHandshakeException)
           (software.amazon.awssdk.auth.credentials AwsCredentialsProvider)
           (software.amazon.awssdk.core.exception ApiCallAttemptTimeoutException
                                                  ApiCallTimeoutException SdkClientException)
           (software.amazon.awssdk.core.sync RequestBody)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model PutObjectRequest PutObjectResponse S3Exception)))

(use-fixtures :each
  (fn [test-fn]
    (with-redefs-fn {#'tigris/retry-delay-ms (constantly 0)} test-fn)))

(defn- service-error [status]
  (-> (S3Exception/builder) (.statusCode status) (.message "Test response") (.build)))

(defn- caught [f]
  (try (f) (catch Throwable t t)))

(defn- store [objects overrides]
  (tigris/store
   (merge {:bucket "events" :prefix "retry-test"
           :client (memory-client/client objects)
           :bundle-request (fn [_ keys] (memory-client/tar objects keys))}
          overrides)))

(deftest transient-put-failures-keep-retrying
  (let [failures [(SdkClientException/create "Reset" (SocketException. "Reset"))
                  (SdkClientException/create "Wrapped" (RuntimeException. (IOException. "Reset")))
                  (SocketTimeoutException. "Read timeout")
                  (HttpTimeoutException. "Request timeout")
                  (ApiCallTimeoutException/create 100)
                  (ApiCallAttemptTimeoutException/create 100)
                  (service-error 429) (service-error 500) (service-error 503)
                  (service-error 409)]
        remaining (atom failures)
        attempts (atom 0)
        retries (atom [])
        objects (atom (sorted-map))
        ^S3Client delegate (memory-client/client objects)
        client (reify S3Client
                 (^PutObjectResponse putObject [_ ^PutObjectRequest request ^RequestBody body]
                   (swap! attempts inc)
                   (if-let [failure (first @remaining)]
                     (do (swap! remaining next) (throw failure))
                     (.putObject delegate request body))))
        s (store objects {:client client :on-retry #(swap! retries conj %)})]
    (is (true? (event-store/try-append! s 0 :event)))
    (is (= (inc (count failures)) @attempts))
    (is (= failures (mapv :exception @retries)))
    (is (= (vec (range 1 (inc (count failures)))) (mapv :attempt @retries)))
    (is (every? #(= :put (:op %)) @retries))))

(deftest permanent-client-and-service-errors-are-not-retried
  (doseq [failure [(SdkClientException/create "Unable to load credentials from any provider")
                   (SdkClientException/create "Invalid configuration" (IllegalArgumentException. "Bad endpoint"))
                   (SdkClientException/create "Missing credentials file" (FileNotFoundException. "credentials"))
                   (SdkClientException/create "TLS configuration" (SSLHandshakeException. "Certificate rejected"))
                   (service-error 400) (service-error 403) (service-error 404) (service-error 422)]]
    (let [attempts (atom 0)
          client (reify S3Client
                   (^PutObjectResponse putObject [_ ^PutObjectRequest _request ^RequestBody _body]
                     (swap! attempts inc)
                     (throw failure)))
          s (store (atom (sorted-map))
                   {:client client
                    :on-retry (fn [_] (throw (AssertionError. "Permanent failure was retried")))})]
      (is (identical? failure (caught #(event-store/try-append! s 0 :event))))
      (is (= 1 @attempts)))))

(defn- response-client
  "Exercise the real bundle request/signing/status handling without a network."
  [statuses calls closed]
  (let [remaining (atom statuses)]
    (proxy [HttpClient] []
      (send [_request _handler]
        (let [status (or (first @remaining)
                         (throw (AssertionError. "Unexpected HTTP request")))
              _ (swap! remaining next)
              _ (swap! calls conj status)
              bytes (if (= 200 status)
                      (with-open [in (memory-client/tar (atom {}) [])] (.readAllBytes in))
                      (.getBytes "Test error body" "UTF-8"))
              body (proxy [ByteArrayInputStream] [bytes]
                     (close [] (swap! closed conj status) (proxy-super close)))]
          (reify HttpResponse
            (statusCode [_] status)
            (body [_] body)))))))

(deftest bundle-throttling-and-server-errors-are-retried
  (let [calls (atom []) closed (atom []) retries (atom [])
        statuses [429 500 503 200]
        s (assoc (store (atom (sorted-map))
                        {:access-key-id "test" :secret-access-key "test"
                         :bundle-request bundle/request!
                         :on-retry #(swap! retries conj %)})
                 :http-client (response-client statuses calls closed))]
    (is (= [] (into [] (event-store/events s 0))))
    (is (= statuses @calls))
    (is (= statuses @closed))
    (is (= [429 500 503] (mapv #(-> % :exception ex-data :status) @retries)))
    (is (every? #(= :bundle (:op %)) @retries))))

(deftest other-bundle-statuses-are-terminal
  (doseq [status [400 403 404 409]]
    (let [calls (atom []) closed (atom [])
          s (assoc (store (atom (sorted-map))
                          {:access-key-id "test" :secret-access-key "test"
                           :bundle-request bundle/request!
                           :on-retry (fn [_] (throw (AssertionError. "Permanent HTTP status was retried")))})
                   :http-client (response-client [status] calls closed))
          t (caught #(into [] (event-store/events s 0)))]
      (is (= :incorrect (:error (ex-data t))))
      (is (= status (:status (ex-data t))))
      (is (= [status] @calls))
      (is (= [status] @closed)))))

(deftest bundle-credential-configuration-errors-are-terminal
  (let [failure (SdkClientException/create "No credentials configured")
        attempts (atom 0)
        s (assoc (store (atom (sorted-map))
                        {:bundle-request bundle/request!
                         :on-retry (fn [_] (throw (AssertionError. "Credentials failure was retried")))})
                 :credentials-provider
                 (reify AwsCredentialsProvider
                   (resolveCredentials [_] (swap! attempts inc) (throw failure))))]
    (is (identical? failure (caught #(into [] (event-store/events s 0)))))
    (is (= 1 @attempts))))
