(ns simplemono.event-store
  "Append-only event log on S3-compatible object storage.

   One store is one stream. Everything it owns lives under one prefix:

     {prefix}/events/{inverted-19d}   one gzip-EDN object per event
     {prefix}/packs/{pack-size}/{inverted-19d}
                                      one gzip-EDN vector per :pack-size events

   Event numbers are zero-based and gap-free. `try-append!` is create-only:
   it returns true when the event was written and false when another writer
   already took that number. The caller decides the number, which is normally
   its read-model cursor plus one, so a lost append means the state the caller
   decided on has moved and it should catch up and decide again.

   Packing is an implementation detail. Completing a range starts a background
   thread that writes the packs, and `get-event` reads from a pack whenever one
   covers the number. Event objects are never deleted, so a pack that is
   missing, stale or corrupt only makes reads slower, never wrong. Packs are
   namespaced by their size, so changing :pack-size on an existing stream
   orphans the old packs and re-packs from scratch rather than corrupting
   them.

   Object names use an inverted key-space (Long/MAX_VALUE - n, zero-padded to
   19 digits), so the newest object sorts first and the head is one LIST with
   maxKeys=1.

   Events must be EDN round-trippable values and should stay small: :pack-size
   of them end up in one pack object. Keep large payloads in a blob store and
   put the blob's name in the event."
  (:require [clojure.edn :as edn]
            [clojure.string :as str])
  (:import (java.io ByteArrayOutputStream)
           (java.net URI)
           (java.nio.charset StandardCharsets)
           (java.util.function Consumer)
           (java.util.zip GZIPInputStream GZIPOutputStream)
           (software.amazon.awssdk.auth.credentials AwsBasicCredentials
                                                    DefaultCredentialsProvider
                                                    StaticCredentialsProvider)
           (software.amazon.awssdk.core.exception SdkClientException)
           (software.amazon.awssdk.core.sync RequestBody)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model GetObjectRequest
                                                     HeadObjectRequest
                                                     ListObjectsV2Request
                                                     ListObjectsV2Response
                                                     NoSuchKeyException
                                                     PutObjectRequest
                                                     S3Exception
                                                     S3Object)))

(def ^:private number-width
  "Digits needed for Long/MAX_VALUE, so every inverted number sorts correctly."
  19)

(defn- invert
  [n]
  (- Long/MAX_VALUE (long n)))

(defn- format-number
  [n]
  (format (str "%0" number-width "d") (invert n)))

(defn- parse-number
  [s]
  (invert (Long/parseLong s)))

(defn- normalize-prefix
  [prefix]
  (str/replace (str (or prefix "")) #"^/+|/+$" ""))

(defn- sub-prefix
  [prefix segment]
  (let [prefix (normalize-prefix prefix)]
    (if (str/blank? prefix)
      (str segment "/")
      (str prefix "/" segment "/"))))

(defn- event-key
  [prefix event-number]
  (str (sub-prefix prefix "events") (format-number event-number)))

(defn- packs-prefix
  "Packs are namespaced by their size. Changing :pack-size therefore starts a
   fresh set of packs instead of writing new-size packs at indices that already
   mean something else: the old packs are orphaned rather than corrupt, are
   never read again, and can be removed with one prefix delete."
  [prefix pack-size]
  (str (sub-prefix prefix "packs") pack-size "/"))

(defn- pack-key
  [prefix pack-size pack-index]
  (str (packs-prefix prefix pack-size) (format-number pack-index)))

(defn- key->number
  [prefix key]
  (when (str/starts-with? key prefix)
    (let [segment (subs key (count prefix))]
      (when (re-matches (re-pattern (str "\\d{" number-width "}")) segment)
        (parse-number segment)))))

(defn- gzip-bytes
  [s]
  (let [out (ByteArrayOutputStream.)]
    (with-open [gzip (GZIPOutputStream. out)]
      (.write gzip (.getBytes (str s) StandardCharsets/UTF_8)))
    (.toByteArray out)))

(defn- conflict?
  [^S3Exception e]
  (contains? #{409 412} (.statusCode e)))

(defn- not-found?
  [^S3Exception e]
  (= 404 (.statusCode e)))

(defn- override
  [headers create-only?]
  (reify Consumer
    (accept [_ builder]
      (doseq [[k v] headers]
        (.putHeader builder (str k) (str v)))
      (when create-only?
        (.putHeader builder "If-None-Match" "*")))))

(defn- put!
  "Create-only put of `bytes` at `key`. Returns true when created, false when
   the key already existed."
  [{:keys [^S3Client client bucket headers]} key bytes]
  (try
    (.putObject client
                (-> (PutObjectRequest/builder)
                    (.bucket bucket)
                    (.key key)
                    (.overrideConfiguration (override headers true))
                    (.contentType "application/edn; charset=utf-8")
                    (.contentEncoding "gzip")
                    (.build))
                (RequestBody/fromBytes bytes))
    true
    (catch S3Exception e
      (if (conflict? e)
        false
        (throw e)))))

(defn- get-edn
  "The gzip-EDN value at `key`, or nil when the object does not exist."
  [{:keys [^S3Client client bucket headers]} key]
  (try
    (with-open [in (.getObject client
                               (-> (GetObjectRequest/builder)
                                   (.bucket bucket)
                                   (.key key)
                                   (.overrideConfiguration (override headers false))
                                   (.build)))
                gzip (GZIPInputStream. in)]
      (edn/read-string (slurp gzip :encoding "UTF-8")))
    (catch NoSuchKeyException _
      nil)
    (catch S3Exception e
      (if (not-found? e)
        nil
        (throw e)))))

(defn- object-exists?
  [{:keys [^S3Client client bucket headers]} key]
  (try
    (.headObject client
                 (-> (HeadObjectRequest/builder)
                     (.bucket bucket)
                     (.key key)
                     (.overrideConfiguration (override headers false))
                     (.build)))
    true
    (catch NoSuchKeyException _
      false)
    (catch S3Exception e
      (if (not-found? e)
        false
        (throw e)))))

(defn- newest-number
  "The highest number under `prefix`, or nil when the prefix is empty. One LIST
   with maxKeys=1: the inverted key-space sorts the newest object first."
  [{:keys [^S3Client client bucket headers]} prefix]
  (let [^ListObjectsV2Response response
        (.listObjectsV2 client
                        (-> (ListObjectsV2Request/builder)
                            (.bucket bucket)
                            (.prefix prefix)
                            (.maxKeys (int 1))
                            (.overrideConfiguration (override headers false))
                            (.build)))]
    (when-let [object (first (.contents response))]
      (key->number prefix (.key ^S3Object object)))))

(defn- gap!
  [event-number]
  (throw (ex-info "Append would create a gap"
                  {:error :gap
                   :event-number event-number})))

(defn- ambiguous!
  [event-number cause]
  (throw (ex-info "Append outcome ambiguous"
                  {:error :ambiguous
                   :event-number event-number}
                  cause)))

(defn latest-event-number
  "The highest event number in the stream, or nil when the stream is empty."
  [{:keys [prefix] :as store}]
  (newest-number store (sub-prefix prefix "events")))

(defn- newest-pack-index
  [{:keys [prefix pack-size] :as store}]
  (newest-number store (packs-prefix prefix pack-size)))

(defn- packed-through
  "The newest pack index this store knows about, or nil. Cached: event objects
   are never deleted, so a stale cache only means a read falls back to the
   individual object."
  [{:keys [state] :as store}]
  (let [cached @state]
    (if (contains? cached :packed-through)
      (:packed-through cached)
      (let [index (newest-pack-index store)]
        (swap! state assoc :packed-through index)
        index))))

(defn- read-pack
  "The events of `pack-index`, or nil when the pack is absent or unusable."
  [{:keys [prefix pack-size state] :as store} pack-index]
  (or (when (= pack-index (:pack-index @state))
        (:pack @state))
      (let [events (get-edn store (pack-key prefix pack-size pack-index))]
        (when (and (vector? events)
                   (= pack-size (count events)))
          (swap! state assoc :pack-index pack-index :pack events)
          events))))

(defn- event-object
  [{:keys [prefix] :as store} event-number]
  (get-edn store (event-key prefix event-number)))

(defn get-event
  "The event at `event-number`, or nil when it does not exist. Served from a
   pack when one covers the number, otherwise from the individual object."
  [{:keys [pack-size] :as store} event-number]
  (let [event-number (long event-number)
        pack-index (quot event-number pack-size)]
    (or (when-some [through (packed-through store)]
          (when (<= pack-index through)
            (some-> (read-pack store pack-index)
                    (nth (mod event-number pack-size) nil))))
        (event-object store event-number))))

(defn- write-pack!
  [{:keys [prefix pack-size] :as store} pack-index]
  (let [from (* (long pack-index) pack-size)
        events (mapv (fn [n]
                       (or (event-object store n)
                           (throw (ex-info "Cannot pack a range with a missing event"
                                           {:error :missing-event
                                            :event-number n
                                            :pack-index pack-index}))))
                     (range from (+ from pack-size)))]
    (put! store (pack-key prefix pack-size pack-index) (gzip-bytes (pr-str events)))))

(defn- pack-completed-ranges!
  "Write every full pack that does not exist yet, oldest first. Packs are
   written in ascending order and creation is create-only, so concurrent
   packers are safe and an interrupted run resumes where it stopped."
  [{:keys [pack-size state] :as store}]
  (when-some [head (latest-event-number store)]
    (let [full-packs (quot (inc (long head)) pack-size)
          start (if-some [through (newest-pack-index store)]
                  (inc through)
                  0)]
      (doseq [pack-index (range start full-packs)]
        (write-pack! store pack-index)
        (swap! state assoc :packed-through pack-index)))))

(defn- pack!
  [{:keys [pack-async? on-pack-error] :as store}]
  (let [run (fn []
              (try
                (pack-completed-ranges! store)
                (catch Throwable t
                  (on-pack-error t))))]
    (if pack-async?
      (doto (Thread. ^Runnable run "simplemono-event-store-pack")
        (.setDaemon true)
        (.start))
      (run))
    nil))

(defn- disambiguate!
  "The put gave no definitive answer. The object decides: our own event means
   the put landed, a different event means another writer won, and nothing at
   all leaves the outcome genuinely unknown."
  [store event-number event cause]
  (let [stored (event-object store event-number)]
    (cond
      (nil? stored) (ambiguous! event-number cause)
      (= stored event) true
      :else false)))

(defn try-append!
  "Create-only append of `event` at `event-number`.

   Returns true when the event was written and false when another writer
   already took that number. Throws {:error :gap} when the previous event is
   missing and {:error :ambiguous} when the outcome could not be determined.

   Checks the previous event with HEAD rather than listing the stream: LIST is
   a Class A operation on object stores such as Tigris while HEAD is Class B,
   roughly ten times cheaper."
  [{:keys [prefix pack-size pack?] :as store} event-number event]
  (let [event-number (long event-number)]
    (when (neg? event-number)
      (throw (ex-info "Event numbers are zero-based"
                      {:error :incorrect
                       :event-number event-number})))
    (if (or (zero? event-number)
            (object-exists? store (event-key prefix (dec event-number))))
      (let [appended (try
                       (put! store (event-key prefix event-number) (gzip-bytes (pr-str event)))
                       (catch SdkClientException e
                         (disambiguate! store event-number event e)))]
        (when (and appended
                   pack?
                   (zero? (mod (inc event-number) pack-size)))
          (pack! store))
        appended)
      (gap! event-number))))

(defn- print-pack-error
  [^Throwable t]
  (binding [*out* *err*]
    (println "simplemono.event-store: packing failed:" (.getMessage t))
    (.printStackTrace t)))

(defn client
  "An AWS SDK S3 client for an S3-compatible object store.

   Options:
   - :endpoint            endpoint override, e.g. https://t3.storage.dev
   - :region              defaults to us-east-1
   - :access-key-id       static credentials; omit to use the default provider
   - :secret-access-key
   - :path-style?         true for stores that need path-style URLs, e.g. MinIO"
  [{:keys [endpoint region access-key-id secret-access-key path-style?]
    :or {region "us-east-1"}}]
  (let [builder (S3Client/builder)]
    (.region builder (Region/of region))
    (if access-key-id
      (.credentialsProvider builder
                            (StaticCredentialsProvider/create
                             (AwsBasicCredentials/create access-key-id
                                                         secret-access-key)))
      (.credentialsProvider builder (DefaultCredentialsProvider/create)))
    (when endpoint
      (.endpointOverride builder (if (instance? URI endpoint)
                                   endpoint
                                   (URI/create (str endpoint)))))
    (when (some? path-style?)
      (.serviceConfiguration builder
                             (reify Consumer
                               (accept [_ config-builder]
                                 (.pathStyleAccessEnabled config-builder
                                                          (boolean path-style?))))))
    (.build builder)))

(defn store
  "An event store for one stream under `:prefix` in `:bucket`.

   Required:
   - :client      an S3Client, e.g. from `client` or
                  `simplemono.event-store.memory-client/client`
   - :bucket
   - :prefix      the stream's prefix; events/ and packs/ are created under it

   Optional:
   - :headers        extra request headers, e.g. {\"X-Tigris-Consistent\" \"true\"}
   - :pack-size      events per pack, default 1000
   - :pack?          set false to never pack, default true
   - :pack-async?    set false to pack on the appending thread, default true
   - :on-pack-error  called with the Throwable when background packing fails,
                     defaults to printing it to *err*"
  [{:keys [client bucket prefix headers pack-size pack? pack-async? on-pack-error]
    :or {pack-size 1000
         pack? true
         pack-async? true}}]
  (when-not client
    (throw (ex-info "An event store requires :client" {:error :incorrect})))
  (when (str/blank? (str bucket))
    (throw (ex-info "An event store requires :bucket" {:error :incorrect})))
  (when-not (pos-int? pack-size)
    (throw (ex-info ":pack-size must be a positive integer"
                    {:error :incorrect
                     :pack-size pack-size})))
  {:client client
   :bucket bucket
   :prefix (normalize-prefix prefix)
   :headers (or headers {})
   :pack-size pack-size
   :pack? pack?
   :pack-async? pack-async?
   :on-pack-error (or on-pack-error print-pack-error)
   :state (atom {})})

(comment

  (require '[simplemono.event-store.memory-client :as memory-client])

  (def s (store {:client (memory-client/client)
                 :bucket "events"
                 :prefix "org/acme"
                 :pack-size 4
                 :pack-async? false}))

  (latest-event-number s)

  (try-append! s 0 {:event/type :example/created})
  (try-append! s 0 {:event/type :example/created})

  (get-event s 0)

  ;; Append until the first pack is complete, then read through it:
  (doseq [n (range 1 8)]
    (try-append! s n {:event/type :example/updated :n n}))

  (latest-event-number s)
  (get-event s 3)

  )
