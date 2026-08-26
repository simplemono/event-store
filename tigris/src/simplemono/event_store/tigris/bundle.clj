(ns simplemono.event-store.tigris.bundle
  "The Tigris bundle API: fetch many objects in one request.

   `POST /{bucket}?bundle` with a list of keys streams back a tar holding those
   objects, in the order asked for. Up to 5000 keys per request, so replaying a
   stream costs one request per 5000 events instead of one per event.

   It is a Tigris extension rather than an S3 operation, so `S3Client` has no
   method for it: the request is built and signed here with the same SigV4
   machinery the SDK uses, then performed with the JDK's HTTP client so the
   response body can be streamed.

   The JDK has no tar support, and a bundle needs very little of the format —
   entries are read in order, sizes are known, and nothing is written — so the
   reader here handles exactly the 512-byte header, the ustar name prefix and
   the padding, and ignores everything else."
  (:require [clojure.string :as str])
  (:import (java.io InputStream)
           (java.net URI)
           (java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers)
           (java.nio.charset StandardCharsets)
           (java.util.function Consumer)
           (software.amazon.awssdk.http ContentStreamProvider SdkHttpFullRequest
                                        SdkHttpMethod)
           (software.amazon.awssdk.http.auth.aws.signer AwsV4HttpSigner)))

(def max-keys
  "The most keys Tigris accepts in one bundle request."
  5000)

(def ^:private block-size 512)

(defn- read-fully
  "Fill `buf` from `in`. True when it was filled, false at a clean end of
   stream, and throws when the stream ends part-way through a block."
  [^InputStream in ^bytes buf]
  (let [len (alength buf)]
    (loop [offset 0]
      (if (= offset len)
        true
        (let [n (.read in buf offset (- len offset))]
          (cond
            (neg? n) (if (zero? offset)
                       false
                       (throw (ex-info "Truncated tar entry"
                                       {:error :truncated-bundle
                                        :expected len
                                        :got offset})))
            :else (recur (+ offset n))))))))

(defn- trimmed
  [^bytes block from len]
  (let [end (loop [i from]
              (if (or (= i (+ from len))
                      (zero? (aget block i)))
                i
                (recur (inc i))))]
    (String. block from (- end from) StandardCharsets/UTF_8)))

(defn- octal
  [^bytes block from len]
  (let [s (str/trim (trimmed block from len))]
    (if (str/blank? s)
      0
      (Long/parseLong s 8))))

(defn- zero-block?
  [^bytes block]
  (every? zero? block))

(defn- entry-name
  "The full name of the entry: ustar splits long names into a prefix at 345 and
   the remainder at 0."
  [^bytes block]
  (let [prefix (trimmed block 345 155)
        name (trimmed block 0 100)]
    (if (str/blank? prefix)
      name
      (str prefix "/" name))))

(defn- skip-padding!
  [^InputStream in size]
  (let [padding (mod (- block-size (mod size block-size)) block-size)]
    (when (pos? padding)
      (read-fully in (byte-array padding)))))

(defn reduce-tar
  "Reduce `f` over the entries of the tar in `in`, as [name bytes], in order.

   `f` may return `reduced` to stop, which leaves the stream part-read — the
   caller owns closing it. Returns the accumulator, deref'd."
  [^InputStream in f init]
  (let [header (byte-array block-size)]
    (loop [acc init]
      (if (reduced? acc)
        @acc
        (if-not (read-fully in header)
          acc
          (if (zero-block? header)
            acc
            (let [name (entry-name header)
                  size (octal header 124 12)
                  content (byte-array size)]
              (when (pos? size)
                (read-fully in content))
              (skip-padding! in size)
              (recur (f acc [name content])))))))))

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

(defn request!
  "POST the bundle and return the tar as an InputStream. The caller closes it.

   `on-error` is \"skip\" — a key with no object is left out of the archive
   rather than failing the whole request, which is how a replay discovers the
   end of a stream: it asks for a full batch and gets back however many exist."
  [{:keys [^HttpClient http-client bucket endpoint headers] :as config} keys]
  (let [host (str/replace (str endpoint) #"^https?://" "")
        url (str "https://" bucket "." host "/?bundle")
        body (.getBytes (json-keys keys) StandardCharsets/UTF_8)
        builder (-> (HttpRequest/newBuilder)
                    (.uri (URI/create url))
                    (.POST (HttpRequest$BodyPublishers/ofByteArray body)))]
    (doseq [[k vs] (signed-headers config url body)
            v vs]
      (.header builder (str k) (str v)))
    (doseq [[k v] headers]
      (.header builder (str k) (str v)))
    (.header builder "x-tigris-bundle-format" "tar")
    (.header builder "x-tigris-bundle-on-error" "skip")
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
