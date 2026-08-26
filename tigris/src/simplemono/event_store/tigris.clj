(ns simplemono.event-store.tigris
  "`simplemono.event-store/EventStore` and `EventReplay` on Tigris.

   One store is one stream, under one prefix in one bucket:

     {prefix}/events/{inverted-19d}   one gzip-EDN object per event

   Event numbers are zero-based and gap-free. `try-append!` is create-only: it
   returns true when the event was written and false when another writer
   already took that number. The caller decides the number, which is normally
   its read-model cursor plus one, so a lost append means the state the caller
   decided on has moved and it should catch up and decide again.

   Replaying does not read one object per event. Tigris can return many objects
   as one streaming tar — see `simplemono.event-store.tigris.bundle` — so
   `reduce-events` costs one request per :bundle-size events, 5000 by default,
   plus one LIST for the head that bounds the last batch. Nothing is written to
   make that fast: there are no packs, nothing to build, nothing to keep
   current, and the first replay of a stream is as cheap as the tenth.

   Every request but the bundle is routed through the leader with
   X-Tigris-Consistent, so a replay on one machine sees what another wrote a
   moment ago. The bundle is not: it costs about five times the latency there,
   and event objects are immutable and create-only, so an eventually consistent
   read can only be missing an object, never showing an old version of one. A
   batch that comes back short for a reason the reducing function did not cause
   is read again through the leader, and fails loudly if it is still short —
   so the cheap path is taken every time and paid for only when it is wrong.

   Transient failures never reach the caller. Every request is retried, with
   backoff, until Tigris answers: a client-side exception, a 429 or a 5xx means
   try again, while a 4xx means the request itself is wrong and is thrown at
   once, so a bad key or a missing bucket fails loudly instead of hanging
   forever. Retries are announced through :on-retry, and the loop sleeps, so
   interrupting the thread ends it.

   Retrying an append is safe because the put is create-only. What a retry
   cannot see by itself is whether the attempt that failed had in fact landed:
   a later attempt then finds the key taken and cannot tell our own write from
   somebody else's. Reading the object back settles it — an equal value was
   ours. That is why events must be EDN round-trippable, and why the caller
   never has to reason about an ambiguous append.

   Object names use an inverted key-space (Long/MAX_VALUE - n, zero-padded to
   19 digits), so the newest object sorts first and the head is one LIST with
   maxKeys=1.

   This targets Tigris rather than S3 in general: the endpoint and the bundle
   API are theirs, and X-Tigris-Consistent is sent by default so that a replay
   sees events another machine wrote a moment ago."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.tigris.bundle :as bundle])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream)
           (java.net URI)
           (java.net.http HttpClient)
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

(def endpoint
  "Tigris speaks S3 at one global endpoint."
  "https://t3.storage.dev")

(def region
  "Tigris routes by itself; the region is only there for the signature."
  "auto")

(def consistent-header
  "Route through the leader so that a read sees a write another machine made a
   moment ago. A replay after a failover, or a projection catching up on a
   second cell, depends on it."
  {"X-Tigris-Consistent" "true"})

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

(defn- event-object
  [{:keys [prefix] :as store} event-number]
  (get-edn store (event-key prefix event-number)))

(defn- append!
  "Create-only append of `event` at `event-number`.

   Checks the previous event with HEAD rather than listing the stream: LIST is
   a Class A operation on object stores such as Tigris while HEAD is Class B,
   roughly ten times cheaper."
  [{:keys [prefix] :as store} event-number event]
  (let [event-number (long event-number)]
    (when (neg? event-number)
      (throw (ex-info "Event numbers are zero-based"
                      {:error :incorrect
                       :event-number event-number})))
    (if (or (zero? event-number)
            (object-exists? store (event-key prefix (dec event-number))))
      (put! store
            (event-key prefix event-number)
            (gzip-bytes (pr-str event))
            event)
      (gap! event-number))))

(defn- print-retry
  [{:keys [op key attempt ^Throwable exception]}]
  (binding [*out* *err*]
    (println (str "simplemono.event-store.s3: " (name op) " " key
                  " failed (attempt " attempt "), retrying: "
                  (.getMessage exception)))))

(defn- bundle-keys
  "The keys for events [from, to], inclusive."
  [prefix from to]
  (mapv #(event-key prefix %) (range (long from) (inc (long to)))))

(defn- decode
  [^bytes gzipped]
  (with-open [gzip (GZIPInputStream. (ByteArrayInputStream. gzipped))]
    (edn/read-string (slurp gzip :encoding "UTF-8"))))

(defn- reduce-bundle
  "Reduce `f` over the events at `keys`, in the order asked for. Returns
   {:acc :read :stopped?}, where :read is how many events reached `f` and
   :stopped? says whether `f` ended it by returning `reduced`.

   Keeping those apart is what makes a short batch meaningful: `f` stopping is
   ordinary, while a batch that ran out of events the head promised means the
   object store is behind.

   Entry names are checked against the keys, because a gap-free stream cannot
   legitimately skip one and a replay that quietly dropped an event would be
   far worse than one that stopped."
  [store keys f init]
  (with-open [tar ((:bundle-request store) store keys)]
    (bundle/reduce-tar
     tar
     (fn [{:keys [acc read] :as state} [name ^bytes content]]
       (if (= "__bundle_errors.json" name)
         (reduced state)
         (let [expected (nth keys read nil)]
           (when-not (= expected name)
             (throw (ex-info "Bundle returned an unexpected object"
                             {:error :missing-event
                              :expected expected
                              :got name})))
           (let [acc (f acc (decode content))
                 state {:acc acc :read (inc (long read)) :stopped? (reduced? acc)}]
             (if (reduced? acc)
               (reduced state)
               state)))))
     {:acc init :read 0 :stopped? false})))

(defn- relaxed
  "The same store, not routed through the leader.

   Only the bundle uses this. Reading through the leader costs about five
   times the latency there, and it buys less than it looks: event objects are
   immutable and create-only, so an eventually consistent read can only be
   missing an object, never showing an old version of one. A missing one is
   detectable — see `fetch-batch` — which is better than paying for it on
   every batch."
  [store]
  (update store :headers dissoc (key (first consistent-header))))

(defn- consistent
  "The same store, routed through the leader."
  [store]
  (update store :headers merge consistent-header))

(defn- fetch-batch
  "One batch, read the cheap way, and read again through the leader when the
   cheap read came up short for a reason `f` did not cause.

   A short batch means the head promised events the bundle did not return. In
   one region that should never happen; across regions it is a replica that
   has not caught up. Either way, returning what arrived would silently
   truncate the replay, so the batch is fetched again consistently and, if it
   is still short, the replay fails instead."
  [store keys f init]
  (let [{:keys [read stopped?] :as result} (reduce-bundle (relaxed store) keys f init)]
    (if (or stopped? (= read (count keys)))
      result
      (let [retried (reduce-bundle (consistent store) keys f init)]
        (if (or (:stopped? retried)
                (= (:read retried) (count keys)))
          retried
          (throw (ex-info "The bundle returned fewer events than the stream holds"
                          {:error :missing-event
                           :expected (count keys)
                           :got (:read retried)
                           :from (first keys)})))))))

(def ^:private default-probe-limit
  "How many events a replay reads one at a time past the range it knows exists,
   before it spends a LIST on finding the head.

   Small, because its only job is to keep the common cases off the LIST: an
   idle replay stops on the first read, and a projection one or two events
   behind is done before this runs out. Anything longer has enough work left to
   amortise a LIST, which is no slower than a GET and only costs more.

   A GET and a LIST take about the same time, so this is a price trade, not a
   speed one: single reads are the cheaper way to read a handful of events and
   the dearer way to read many."
  4)

(defn- over-promised!
  [known-head t]
  (throw (ex-info "The stream does not hold the events :known-head promised"
                  {:error :missing-event
                   :known-head known-head}
                  t)))

(defn- bounded-batch
  "One batch inside a range every key of which is known to exist.

   Nothing is asked for that cannot be there, so this costs one request and no
   absent-key time. A short batch here is not the end of the stream — the
   caller promised these events — so it surfaces as the promise being wrong."
  [{:keys [prefix bundle-size] :as store} event-number latest f acc known-head]
  (let [to (min (long latest) (dec (+ (long event-number) (long bundle-size))))
        keys (bundle-keys prefix event-number to)]
    (try
      (fetch-batch store keys f acc)
      (catch clojure.lang.ExceptionInfo t
        (if (and known-head (= :missing-event (:error (ex-data t))))
          (over-promised! known-head t)
          (throw t))))))

(defn- replay
  "Walk the stream, never asking for a key that cannot exist.

   Asking for a full batch and letting Tigris skip what is missing would also
   work, and is how an earlier version found the end of a stream. It is far
   worse than it looks: a skipped key costs about 7ms, so a 5000-key batch past
   the end of a short stream takes 35 seconds and answers with half a megabyte
   naming everything that was not there. Every batch is therefore bounded by a
   number the stream is known to reach.

   That bound comes from one of three places, cheapest first.

   `:known-head` is free. Nothing is ever deleted, so an event number the
   caller has already seen still exists, and everything up to it can be read
   without asking where the stream ends. A projection replaying from its own
   SQLite cursor pays nothing at all to discover the range it already knows.

   Past that, the cheapest question is whether the next event exists, and the
   answer is the event itself: one GET, which is a Class B operation and is
   needed anyway. A replay that finds nothing there — an idle projection
   catching up, which is the common case — costs exactly that one GET and no
   LIST.

   A replay still going after `:probe-limit` single reads has enough left to
   amortise a LIST, so it pays for one and goes back to full batches. LIST is
   no slower than a GET, it is only Class A, so the point is to spend it where
   there is work to spread it over."
  [store from f init opts]
  (let [known-head (:known-head opts)
        probe-limit (long (:probe-limit store))]
    (loop [event-number (long from)
           acc init
           latest (when known-head (long known-head))
           probed 0]
      (cond
        (reduced? acc)
        @acc

        (and latest (<= event-number (long latest)))
        (let [{:keys [acc read stopped?]}
              (bounded-batch store event-number latest f acc
                             (when (= latest (some-> known-head long)) known-head))]
          (if stopped?
            @acc
            (recur (+ event-number (long read)) acc latest 0)))

        (< probed probe-limit)
        (if-some [event (event-object store event-number)]
          (let [acc (f acc event)]
            (if (reduced? acc)
              @acc
              (recur (inc event-number) acc latest (inc probed))))
          acc)

        :else
        (let [grown (head store)]
          (if (and grown (>= (long grown) event-number))
            (recur event-number acc (long grown) 0)
            acc))))))

(defrecord TigrisEventStore [client bucket prefix headers endpoint region
                             credentials-provider http-client bundle-request
                             bundle-size probe-limit on-retry]
  event-store/EventStore
  (try-append! [this event-number event]
    (append! this event-number event))

  (get-event [this event-number]
    (event-object this event-number))

  (latest-event-number [this]
    (head this))

  event-store/EventReplay
  (-reduce-events [this from f init opts]
    (replay this from f init opts)))

(defn- credentials
  [access-key-id secret-access-key]
  (if access-key-id
    (StaticCredentialsProvider/create
     (AwsBasicCredentials/create access-key-id secret-access-key))
    (DefaultCredentialsProvider/create)))

(defn client
  "An S3Client pointed at Tigris.

   Options:
   - :access-key-id       static credentials; omit to use the default provider
   - :secret-access-key"
  [{:keys [access-key-id secret-access-key]}]
  (-> (S3Client/builder)
      (.region (Region/of region))
      (.credentialsProvider (credentials access-key-id secret-access-key))
      (.endpointOverride (URI/create endpoint))
      (.build)))

(defn store
  "An event store for one stream under `:prefix` in `:bucket`.

   Required:
   - :bucket
   - :prefix               the stream's prefix; events/ is created under it
   - :access-key-id        static credentials; omit to use the default provider
   - :secret-access-key

   Optional:
   - :client         an S3Client, if you would rather build it yourself or hand
                     in a test double; built from the credentials otherwise
   - :bundle-request a fn of [store keys] returning a tar InputStream, for
                     tests; the real Tigris bundle request otherwise
   - :headers        extra request headers, merged over X-Tigris-Consistent
   - :bundle-size    events per replay request, default and maximum 5000
   - :probe-limit    events a replay reads one at a time past what it knows
                     exists, before spending a LIST to find the head, default 4.
                     An idle replay stops on the first of these, so it costs one
                     GET and no LIST.
   - :on-retry       called with {:op :key :attempt :exception} before every
                     retry of a transient failure, defaults to printing a line
                     to *err*. An outage is otherwise indistinguishable from
                     slowness, so replace this with your own logging."
  [{:keys [bucket prefix access-key-id secret-access-key
           client bundle-request headers bundle-size probe-limit on-retry]
    :or {bundle-size bundle/max-keys
         probe-limit default-probe-limit}}]
  (when (str/blank? (str bucket))
    (throw (ex-info "An event store requires :bucket" {:error :incorrect})))
  (when-not (and (pos-int? bundle-size)
                 (<= bundle-size bundle/max-keys))
    (throw (ex-info ":bundle-size must be between 1 and the Tigris maximum"
                    {:error :incorrect
                     :bundle-size bundle-size
                     :maximum bundle/max-keys})))
  (when-not (and (integer? probe-limit) (not (neg? (long probe-limit))))
    (throw (ex-info ":probe-limit must be a non-negative integer"
                    {:error :incorrect
                     :probe-limit probe-limit})))
  (map->TigrisEventStore
   {:client (or client (simplemono.event-store.tigris/client
                        {:access-key-id access-key-id
                         :secret-access-key secret-access-key}))
    :bucket bucket
    :prefix (normalize-prefix prefix)
    :headers (merge consistent-header headers)
    :endpoint endpoint
    :region region
    ;; The provider, not resolved credentials: a store handed a :client and a
    ;; :bundle-request never signs anything and must not demand them.
    :credentials-provider (credentials access-key-id secret-access-key)
    :http-client (HttpClient/newHttpClient)
    :bundle-request (or bundle-request bundle/request!)
    :bundle-size bundle-size
    :probe-limit probe-limit
    :on-retry (or on-retry print-retry)}))

(comment

  ;; ==========================================================================
  ;; Offline: a fake S3Client, and a bundle built from the same objects.
  ;; ==========================================================================

  (require '[simplemono.event-store.memory-client :as memory-client])

  (def objects (atom (sorted-map)))

  (def s (store {:bucket "events"
                 :prefix "org/acme"
                 :client (memory-client/client objects)
                 :bundle-request (fn [_store keys]
                                   (memory-client/tar objects keys))
                 :bundle-size 4}))

  (doseq [n (range 9)]
    (event-store/try-append! s n {:event/type :example/happened :n n}))

  (event-store/latest-event-number s)
  (event-store/get-event s 3)
  (event-store/reduce-events s 0 conj [])

  )

(comment

  ;; ==========================================================================
  ;; Using the library against a real Tigris bucket. Evaluate downwards, one
  ;; form at a time. Nothing here reaches inside: it is the public API only,
  ;; so it doubles as the worked example.
  ;;
  ;; Each run mints a fresh prefix, so runs never collide. The library never
  ;; deletes anything, by design — empty the bucket from the Tigris console
  ;; when it gets untidy.
  ;; ==========================================================================

  (defn env
    "Reads env.edn from the project folder, holding :access-key-id,
     :secret-access-key and :bucket. Gitignored."
    []
    (edn/read-string (slurp "env.edn")))

  (keys (env))
  ;;=> (:access-key-id :secret-access-key :bucket)

  (defn a-store
    "A store on a fresh prefix, so runs never collide."
    ([] (a-store {}))
    ([overrides]
     (store (merge (env)
                   {:prefix (str "verify/" (random-uuid))}
                   overrides))))

  (def s (a-store))


  ;; --- the whole protocol --------------------------------------------------

  (event-store/latest-event-number s)
  ;;=> nil, the stream is empty

  (event-store/try-append! s 0 {:event/occurred-at (java.util.Date.)
                                :event/type :verify/first
                                :event/subjects ["/verify/1/"]})
  ;;=> true

  (event-store/get-event s 0)
  (event-store/latest-event-number s)
  ;;=> 0

  (event-store/reduce-events s 0 conj [])


  ;; --- a lost append is not an error ---------------------------------------

  (event-store/try-append! s 0 {:event/type :verify/loser})
  ;;=> false. Another writer holds that number; the stored event is untouched.

  (event-store/get-event s 0)


  ;; --- a gap is ------------------------------------------------------------

  (event-store/try-append! s 5 {:event/type :verify/too-far})
  ;;=> throws {:error :gap}


  ;; --- the intended write path ---------------------------------------------
  ;; Catch a read model up, decide against it, append at its cursor plus one.
  ;; A false means somebody else won, so it catches up and decides again —
  ;; which is the whole reason the decision lives inside the loop. Deciding
  ;; once and retrying the append would re-append a decision made against
  ;; state that has since moved.

  (defn append-once!
    "Appends what `decide` returns, or nothing when it returns nil."
    [store decide]
    (loop []
      (let [{:keys [cursor seen]}
            (event-store/reduce-events store 0
                                       (fn [acc event]
                                         (-> acc
                                             (update :cursor inc)
                                             (update :seen conj (:event/type event))))
                                       {:cursor -1 :seen #{}})]
        (if-some [event (decide seen)]
          (if (event-store/try-append! store (inc cursor) event)
            {:appended (:event/type event) :at (inc cursor)}
            (recur))
          :nothing-to-append))))

  (defn only-once
    [seen]
    (when-not (contains? seen :verify/only-once)
      {:event/occurred-at (java.util.Date.)
       :event/type :verify/only-once}))

  (append-once! s only-once)
  ;;=> {:appended :verify/only-once :at 1}

  (append-once! s only-once)
  ;;=> :nothing-to-append — the decision saw its own event.
  ;;   That is the precondition, and it needs no query language.


  ;; --- replaying -----------------------------------------------------------

  (doseq [n (range 2 20)]
    (event-store/try-append! s n {:event/occurred-at (java.util.Date.)
                                  :event/type :verify/happened
                                  :event/n n}))

  (mapv :event/n (event-store/reduce-events s 0 conj []))
  ;;=> [nil nil 2 3 ... 19], in order

  (event-store/reduce-events s 15 conj [])
  ;;=> a replay can start anywhere

  (event-store/reduce-events s 0
                             (fn [acc e]
                               (if (= 3 (count acc)) (reduced acc) (conj acc e)))
                             [])
  ;;=> reduced stops it, and stops fetching

  (event-store/reduce-events s 99 conj [])
  ;;=> [], past the end


  ;; --- does a replay see a write that just happened ------------------------
  ;; The design leans on this: a projection catching up on one machine has to
  ;; see what another wrote a moment ago. X-Tigris-Consistent is sent by
  ;; default; the second store below turns it off. Run each several times, a
  ;; single pass proves nothing about a race.

  (defn append-then-replay!
    [store]
    (let [n (inc (long (or (event-store/latest-event-number store) -1)))
          marker (random-uuid)]
      (event-store/try-append! store n {:event/type :verify/fresh
                                        :marker marker})
      (->> (event-store/reduce-events store 0 conj [])
           (some #(= marker (:marker %)))
           boolean)))

  (frequencies (repeatedly 10 #(append-then-replay! s)))
  ;;=> {true 10} is what the design needs

  (def unguarded (a-store {:prefix (:prefix s)
                           :headers {"X-Tigris-Consistent" "false"}}))

  (frequencies (repeatedly 10 #(append-then-replay! unguarded)))
  ;;=> if false ever shows up here while the run above is always true, the
  ;;   header is doing real work and has to stay


  ;; --- a replay versus one request per event -------------------------------
  ;; Appending is sequential by construction, so the first form takes a while.

  (def big (a-store))

  (time
   (doseq [n (range 200)]
     (event-store/try-append! big n {:event/occurred-at (java.util.Date.)
                                     :event/type :verify/bulk
                                     :event/n n
                                     :payload (apply str (repeat 200 "x"))})))

  (time (count (event-store/reduce-events big 0 conj [])))
  ;;=> one bundle request, plus one LIST for the head

  (time (count (event-store/reduce-by-get big 0 conj [])))
  ;;=> the generic path every implementation gets: 201 requests

  ;; The Tigris console shows what each cost, and which class a bundle
  ;; request bills as.

  )
