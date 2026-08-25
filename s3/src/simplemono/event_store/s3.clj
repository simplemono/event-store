(ns simplemono.event-store.s3
  "`simplemono.event-store/EventStore` on S3-compatible object storage.

   One store is one stream. Everything it owns lives under one prefix:

     {prefix}/events/{inverted-19d}   one gzip-EDN object per event
     {prefix}/packs/{pack-size}/{inverted-19d}
                                      one gzip-EDN vector per :pack-size events

   Event numbers are zero-based and gap-free. `try-append!` is create-only:
   it returns true when the event was written and false when another writer
   already took that number. The caller decides the number, which is normally
   its read-model cursor plus one, so a lost append means the state the caller
   decided on has moved and it should catch up and decide again.

   Transient failures never reach the caller. Every request is retried, with
   backoff, until the object store answers: a client-side exception, a 429 or a
   5xx means try again, while a 4xx means the request itself is wrong and is
   thrown at once, so a bad key or a missing bucket fails loudly instead of
   hanging forever. Retries are announced through :on-retry, and the loop
   sleeps, so interrupting the thread ends it.

   Retrying an append is safe because the put is create-only. What a retry
   cannot see by itself is whether the attempt that failed had in fact landed:
   a later attempt then finds the key taken and cannot tell our own write from
   somebody else's. Reading the object back settles it — an equal value was
   ours. That is why events must be EDN round-trippable, and why the caller
   never has to reason about an ambiguous append.

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

   Events should stay small: :pack-size of them end up in one pack object.
   Keep large payloads in a blob store and put the blob's name in the event."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [simplemono.event-store :as event-store])
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

(defn- transient-failure?
  "True when the object store may answer differently next time. A client-side
   exception is a network or timeout problem, 429 is throttling and 5xx is the
   store's own trouble. Everything else — a bad key, a missing bucket, a
   malformed request — is the caller's problem and must not be retried, or a
   configuration error turns into a silent hang."
  [t]
  (or (instance? SdkClientException t)
      (and (instance? S3Exception t)
           (let [status (.statusCode ^S3Exception t)]
             (or (= 429 status)
                 (<= 500 status))))))

(defn- retry-delay-ms
  "Exponential backoff from 100ms, capped at 30s, with jitter so that writers
   which failed together do not come back in lockstep."
  [attempt]
  (+ (rand-int 100)
     (min 30000 (* 100 (bit-shift-left 1 (min (long attempt) 8))))))

(defn- await-retry!
  "Announce the failed attempt, then sleep before the next one. Sleeping is
   what makes the loop interruptible: interrupting the thread ends it."
  [{:keys [on-retry]} op key attempt ^Throwable t]
  (on-retry {:op op
             :key key
             :attempt attempt
             :exception t})
  (Thread/sleep (retry-delay-ms (dec (long attempt)))))

(defn- with-retry
  "Call `thunk` until the object store answers, retrying transient failures."
  [store op key thunk]
  (loop [attempt 1]
    (let [outcome (try
                    {:value (thunk)}
                    (catch Throwable t
                      (if (transient-failure? t)
                        {:failure t}
                        (throw t))))]
      (if-some [t (:failure outcome)]
        (do
          (await-retry! store op key attempt t)
          (recur (inc attempt)))
        (:value outcome)))))

(defn- override
  [headers create-only?]
  (reify Consumer
    (accept [_ builder]
      (doseq [[k v] headers]
        (.putHeader builder (str k) (str v)))
      (when create-only?
        (.putHeader builder "If-None-Match" "*")))))

(defn- get-edn
  "The gzip-EDN value at `key`, or nil when the object does not exist."
  [{:keys [^S3Client client bucket headers] :as store} key]
  (try
    (with-retry
      store :get key
      (fn []
        (with-open [in (.getObject client
                                   (-> (GetObjectRequest/builder)
                                       (.bucket bucket)
                                       (.key key)
                                       (.overrideConfiguration (override headers false))
                                       (.build)))
                    gzip (GZIPInputStream. in)]
          (edn/read-string (slurp gzip :encoding "UTF-8")))))
    (catch NoSuchKeyException _
      nil)
    (catch S3Exception e
      (if (not-found? e)
        nil
        (throw e)))))

(defn- put-once!
  "One create-only put. True when created, false when the key already existed."
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

(defn- put!
  "Create-only put of `bytes` at `key`, retrying until the object store
   answers. True when this store created the object, false when it already
   existed.

   An attempt that failed transiently may still have landed. When a later
   attempt then finds the key taken, `value` decides whose write it was: an
   equal stored value was ours."
  [store key bytes value]
  (loop [attempt 1
         uncertain? false]
    (let [outcome (try
                    {:created? (put-once! store key bytes)}
                    (catch Throwable t
                      (if (transient-failure? t)
                        {:failure t}
                        (throw t))))]
      (if-some [t (:failure outcome)]
        (do
          (await-retry! store :put key attempt t)
          (recur (inc attempt) true))
        (let [created? (:created? outcome)]
          (if (and (false? created?) uncertain?)
            (= value (get-edn store key))
            created?))))))

(defn- object-exists?
  [{:keys [^S3Client client bucket headers] :as store} key]
  (try
    (with-retry
      store :head key
      (fn []
        (.headObject client
                     (-> (HeadObjectRequest/builder)
                         (.bucket bucket)
                         (.key key)
                         (.overrideConfiguration (override headers false))
                         (.build)))
        true))
    (catch NoSuchKeyException _
      false)
    (catch S3Exception e
      (if (not-found? e)
        false
        (throw e)))))

(defn- newest-number
  "The highest number under `prefix`, or nil when the prefix is empty. One LIST
   with maxKeys=1: the inverted key-space sorts the newest object first."
  [{:keys [^S3Client client bucket headers] :as store} prefix]
  (with-retry
    store :list prefix
    (fn []
      (let [^ListObjectsV2Response response
            (.listObjectsV2 client
                            (-> (ListObjectsV2Request/builder)
                                (.bucket bucket)
                                (.prefix prefix)
                                (.maxKeys (int 1))
                                (.overrideConfiguration (override headers false))
                                (.build)))]
        (when-let [object (first (.contents response))]
          (key->number prefix (.key ^S3Object object)))))))

(defn- gap!
  [event-number]
  (throw (ex-info "Append would create a gap"
                  {:error :gap
                   :event-number event-number})))

(defn- head
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

(defn- read-event
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
    (put! store
          (pack-key prefix pack-size pack-index)
          (gzip-bytes (pr-str events))
          events)))

(defn- pack-completed-ranges!
  "Write every full pack that does not exist yet, oldest first. Packs are
   written in ascending order and creation is create-only, so concurrent
   packers are safe and an interrupted run resumes where it stopped."
  [{:keys [pack-size state] :as store}]
  (when-some [latest (head store)]
    (let [full-packs (quot (inc (long latest)) pack-size)
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

(defn- append!
  "Create-only append of `event` at `event-number`.

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
      (let [appended (put! store
                           (event-key prefix event-number)
                           (gzip-bytes (pr-str event))
                           event)]
        (when (and appended
                   pack?
                   (zero? (mod (inc event-number) pack-size)))
          (pack! store))
        appended)
      (gap! event-number))))

(defn- print-retry
  [{:keys [op key attempt ^Throwable exception]}]
  (binding [*out* *err*]
    (println (str "simplemono.event-store.s3: " (name op) " " key
                  " failed (attempt " attempt "), retrying: "
                  (.getMessage exception)))))

(defn- print-pack-error
  [^Throwable t]
  (binding [*out* *err*]
    (println "simplemono.event-store.s3: packing failed:" (.getMessage t))
    (.printStackTrace t)))

(defrecord S3EventStore [client bucket prefix headers
                         pack-size pack? pack-async? on-pack-error on-retry
                         state]
  event-store/EventStore
  (try-append! [this event-number event]
    (append! this event-number event))

  (get-event [this event-number]
    (read-event this event-number))

  (latest-event-number [this]
    (head this)))

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
                     defaults to printing it to *err*
   - :on-retry       called with {:op :key :attempt :exception} before every
                     retry of a transient failure, defaults to printing a line
                     to *err*. An outage is otherwise indistinguishable from
                     slowness, so replace this with your own logging."
  [{:keys [client bucket prefix headers pack-size pack? pack-async?
           on-pack-error on-retry]
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
  (->S3EventStore client
                  bucket
                  (normalize-prefix prefix)
                  (or headers {})
                  pack-size
                  pack?
                  pack-async?
                  (or on-pack-error print-pack-error)
                  (or on-retry print-retry)
                  (atom {})))

(comment

  (require '[simplemono.event-store.memory-client :as memory-client])

  (def s (store {:client (memory-client/client)
                 :bucket "events"
                 :prefix "org/acme"
                 :pack-size 4
                 :pack-async? false}))

  (event-store/latest-event-number s)

  (event-store/try-append! s 0 {:event/type :example/created})
  (event-store/try-append! s 0 {:event/type :example/created})

  (event-store/get-event s 0)

  ;; Append until the first pack is complete, then read through it:
  (doseq [n (range 1 8)]
    (event-store/try-append! s n {:event/type :example/updated :n n}))

  (event-store/latest-event-number s)
  (event-store/get-event s 3)

  )
