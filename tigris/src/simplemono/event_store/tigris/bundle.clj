(ns simplemono.event-store.tigris.bundle
  "The Tigris bundle API: fetch many objects in one request.

   `POST /{bucket}?bundle` with a list of keys streams back a tar holding those
   objects, in the order asked for. Up to 5000 keys per request, so replaying a
   stream costs one request per 5000 events instead of one per event.

   It is a Tigris extension rather than an S3 operation, so `S3Client` has no
   method for it: the request is built and signed here with the same SigV4
   machinery the SDK uses, then performed with the JDK's HTTP client so the
   response body can be streamed.

   The archive is read with Apache Commons Compress rather than by hand. The
   format is Tigris's to choose, and they do choose: a key that fits is a plain
   ustar entry, a longer one is split into the ustar name prefix, and past the
   256 characters ustar can hold they switch to a POSIX pax extended header.
   A reader written against what we happened to observe would be a guess, and
   that one has already been wrong once."
  (:require [clojure.string :as str])
  (:import (java.io InputStream)
           (java.net URI)
           (java.time Duration)
           (java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers)
           (java.nio.charset StandardCharsets)
           (java.util.function Consumer)
           (org.apache.commons.compress.archivers.tar TarArchiveInputStream)
           (software.amazon.awssdk.http ContentStreamProvider SdkHttpFullRequest
                                        SdkHttpMethod)
           (software.amazon.awssdk.http.auth.aws.signer AwsV4HttpSigner)))

(def max-keys
  "The most keys Tigris accepts in one bundle request."
  5000)

(defn- entry-bytes
  "The whole of the current entry. Entries are one event each and small by
   design, so reading one into memory is bounded by the size guidance rather
   than by the archive."
  [^TarArchiveInputStream tar size]
  (let [buffer (byte-array size)]
    (loop [offset 0]
      (if (= offset size)
        buffer
        (let [n (.read tar buffer offset (- size offset))]
          (cond
            (neg? n) (throw (ex-info "Truncated tar entry"
                                     {:error :truncated-bundle
                                      :expected size
                                      :got offset}))
            (zero? n) (throw (ex-info "Bundle stream stalled"
                                      {:error :unavailable
                                       :expected size
                                       :got offset}))
            :else (recur (+ offset n))))))))

(defn reduce-tar
  "Reduce `f` over the entries of the tar in `in`, as [name bytes], in order.

   `f` may return `reduced` to stop, which leaves the stream part-read — the
   caller owns closing it. Returns the accumulator, deref'd."
  [^InputStream in f init]
  (let [tar (TarArchiveInputStream. in)]
    (loop [acc init]
      (if (reduced? acc)
        @acc
        (if-some [entry (.getNextEntry tar)]
          (if (.isFile entry)
            (recur (f acc [(.getName entry)
                           (entry-bytes tar (.getSize entry))]))
            (recur acc))
          acc)))))

(defn- signed-headers
  "SigV4 headers for the bundle POST. The body is a key list we just built, so
   hashing it costs nothing and no streaming signature is needed."
  [{:keys [credentials-provider region]} ^String url ^bytes body]
  (let [request (-> (SdkHttpFullRequest/builder)
                    (.method SdkHttpMethod/POST)
                    (.uri (URI/create url))
                    (.putHeader "content-type" "application/json")
                    (.build))
        payload (reify ContentStreamProvider
                  (newStream [_] (java.io.ByteArrayInputStream. body)))
        signed (.sign (AwsV4HttpSigner/create)
                      ^Consumer
                      (reify Consumer
                        (accept [_ builder]
                          (doto builder
                            (.identity (.resolveCredentials credentials-provider))
                            (.request request)
                            (.payload payload)
                            (.putProperty AwsV4HttpSigner/SERVICE_SIGNING_NAME "s3")
                            (.putProperty AwsV4HttpSigner/REGION_NAME region)))))]
    (.headers (.request signed))))

(defn- json-keys
  "The request body. Object keys hold no character JSON needs escaped beyond a
   quote, but escaping is cheap and a key is caller data."
  [keys]
  (str "{\"keys\":["
       (str/join "," (map (fn [k]
                            (str \" (str/escape (str k) {\" "\\\"" \\ "\\\\"}) \"))
                          keys))
       "]}"))

(def ^:private restricted-headers
  "Headers the JDK's HttpClient manages itself and refuses to let a caller set.

   Host is among them and is also part of the SigV4 signature, but the JDK
   derives it from the same URI we signed, so the header that goes out is
   identical and dropping it here changes nothing."
  #{"connection" "content-length" "expect" "host" "upgrade"})

(defn settable-headers
  "The headers to put on the request: everything signed, then ours, minus the
   ones the JDK insists on owning. Returns a seq of [name value]."
  [signed extra]
  (concat
   (for [[k vs] signed
         :when (not (restricted-headers (str/lower-case (str k))))
         v vs]
     [(str k) (str v)])
   (for [[k v] extra
         :when (not (restricted-headers (str/lower-case (str k))))]
     [(str k) (str v)])))

(defn request!
  "POST the bundle and return the tar as an InputStream. The caller closes it.

   `on-error` is \"skip\" — a key with no object is left out of the archive
   rather than failing the whole request. A replay bounds its batches by the
   head and so does not rely on that, but it means one object disappearing
   under a reader degrades to a short archive instead of a failed request."
  [{:keys [^HttpClient http-client bucket endpoint headers] :as config} keys]
  (let [host (str/replace (str endpoint) #"^https?://" "")
        url (str "https://" bucket "." host "/?bundle")
        body (.getBytes (json-keys keys) StandardCharsets/UTF_8)
        builder (-> (HttpRequest/newBuilder)
                    (.uri (URI/create url))
                    ;; Tigris allows a bundle fifteen minutes. Without a
                    ;; timeout of our own a stalled connection simply hangs.
                    (.timeout (Duration/ofMinutes 15))
                    (.POST (HttpRequest$BodyPublishers/ofByteArray body)))]
    (doseq [[k v] (settable-headers (signed-headers config url body)
                                    (merge headers
                                           {"x-tigris-bundle-format" "tar"
                                            "x-tigris-bundle-on-error" "skip"}))]
      (.header builder k v))
    (let [response (.send http-client
                          (.build builder)
                          (HttpResponse$BodyHandlers/ofInputStream))
          status (.statusCode response)]
      (if (= 200 status)
        (.body response)
        (let [message (slurp (.body response) :encoding "UTF-8")]
          (throw (ex-info "Bundle request failed"
                          {:error (if (<= 500 status) :unavailable :incorrect)
                           :status status
                           :body message})))))))
