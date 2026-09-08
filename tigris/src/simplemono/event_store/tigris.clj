(ns simplemono.event-store.tigris
  "`simplemono.event-store/EventAppend`, `EventSource` and `EventHead` on
   Tigris.

   One store is one stream, under one prefix in one bucket:

     {prefix}/events/{inverted-19d}   one gzip-EDN object per event

   Event numbers are zero-based and gap-free. `try-append!` is create-only: it
   returns true when the event was written and false when another writer
   already took that number. The caller decides the number, which is normally
   its read-model cursor plus one, so a lost append means the state the caller
   decided on has moved and it should catch up and decide again.

   Reading a range does not cost one request per event. Tigris can return many
   objects as one streaming tar — see `simplemono.event-store.tigris.bundle` —
   so `events` reads a stream in batches of up to a hundred. Nothing is written
   to make that fast: there are no packs, nothing to build, nothing to keep
   current, and the first replay of a stream is as cheap as the tenth.

   A replay opens with one key, spends one LIST on the head only once it has
   found something to read, and then reads up to that head in batches. An idle
   replay stops on the opening key, so it costs one request and no LIST, and
   reading a single event by number costs the same.

   Which reads go through the leader with X-Tigris-Consistent is measured
   rather than chosen. A key that is there costs about 1ms relaxed and 6ms
   through the leader, while a key that is *not* there costs about 250ms
   relaxed and 9ms. So single keys, whose whole purpose is to find out whether
   something exists, go through the leader; batches, whose keys the head has
   already promised, do not. Appends and the head LIST go through the leader
   too, because a stale answer there is indistinguishable from a real one.

   A batch that comes back short is a replica that has not caught up. The keys
   it missed are asked of the leader, and if they are still not there the
   replay throws rather than ending quietly, because the head promised them.

   Transient failures never reach the caller. Every request is retried, with
   backoff, until Tigris answers: a client-side exception, a 429 or a 5xx means
   try again. A conditional PUT's 409 is also retried, and its 412 is resolved
   by checking ownership. Other 4xx responses are thrown at once, so a bad key
   or a missing bucket fails loudly instead of hanging forever. Retries are
   announced through :on-retry, and the loop sleeps, so interrupting the thread
   ends it.

   Retrying an append is safe because the put is create-only. What a retry
   cannot see by itself is whether the attempt that failed had in fact landed:
   a later attempt then finds the key taken and cannot tell our own write from
   somebody else's. Each append invocation writes a fresh UUID in object
   metadata and keeps it across retries. A consistent HEAD on every 412 checks
   that UUID, even when the SDK hid an earlier attempt by retrying internally.
   Ownership never depends on reading or comparing the event body.

   Object names use an inverted key-space (Long/MAX_VALUE - n, zero-padded to
   19 digits), so the newest object sorts first and the head is one LIST with
   maxKeys=1.

   This targets Tigris rather than S3 in general: the endpoint and the bundle
   API are theirs, and X-Tigris-Consistent is sent by default so that a replay
   sees events another machine wrote a moment ago."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [simplemono.event-store :as event-store]
            [simplemono.event-store.util :as util]
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
           (software.amazon.awssdk.services.s3.model HeadObjectRequest
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
      ;; The bundle goes out over the JDK's HTTP client rather than the SDK, so
      ;; its network failures arrive as IOException and its 5xx as an ex-info.
      (instance? java.io.IOException t)
      (= :unavailable (:error (ex-data t)))
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

(defn- consistent
  "The same store, routed through the leader."
  [store]
  (update store :headers merge consistent-header))

(defn- object-metadata
  "The metadata at `key`, or nil when the object does not exist."
  [{:keys [^S3Client client bucket headers] :as store} key]
  (try
    (with-retry
      store :head key
      (fn []
        (.metadata (.headObject client
                                (-> (HeadObjectRequest/builder)
                                    (.bucket bucket)
                                    (.key key)
                                    (.overrideConfiguration (override headers false))
                                    (.build))))))
    (catch NoSuchKeyException _
      nil)
    (catch S3Exception e
      (if (not-found? e)
        nil
        (throw e)))))

(defn- put-once!
  "One create-only put. True when created, false when the key already existed."
  [{:keys [^S3Client client bucket headers]} key bytes write-id]
  (try
    (.putObject client
                (-> (PutObjectRequest/builder)
                    (.bucket bucket)
                    (.key key)
                    (.overrideConfiguration (override headers true))
                    (.contentType "application/edn; charset=utf-8")
                    (.contentEncoding "gzip")
                    (.metadata {"event-store-write-id" write-id})
                    (.build))
                (RequestBody/fromBytes bytes))
    true
    (catch S3Exception e
      (if (= 412 (.statusCode e))
        false
        (throw e)))))

(defn- put!
  "Create-only put of `bytes` at `key`. A fresh UUID identifies this invocation
   and stays in the object's metadata across retries. Every 412 is resolved
   with a consistent HEAD: the SDK may have retried without telling us, and
   equal event values do not establish ownership. A 409 only means a write
   conflicted, not that the key exists, so it is retried."
  [store key bytes]
  (let [write-id (str (random-uuid))]
    (loop [attempt 1]
      (let [outcome (try
                      {:created? (put-once! store key bytes write-id)}
                      (catch Throwable t
                        (if (or (transient-failure? t)
                                (and (instance? S3Exception t)
                                     (= 409 (.statusCode ^S3Exception t))))
                          {:failure t}
                          (throw t))))]
        (if-some [t (:failure outcome)]
          (do
            (await-retry! store :put key attempt t)
            (recur (inc attempt)))
          (or (:created? outcome)
              (= write-id (get (object-metadata (consistent store) key)
                               "event-store-write-id"))))))))

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
  "One LIST, which is a Class A operation and about ten times the price of a
   read. A replay spends one of these, and only once it has found something to
   read; `latest-event-number` hands the same lookup to callers."
  [{:keys [prefix] :as store}]
  (newest-number store (sub-prefix prefix "events")))

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
            (some? (object-metadata store (event-key prefix (dec event-number)))))
      (put! store
            (event-key prefix event-number)
            (gzip-bytes (pr-str event)))
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
  ;; Only getting hold of the archive is retried. Once entries start reaching
  ;; `f` a retry would hand it the same events twice, so a failure mid-stream
  ;; propagates and the caller resumes from whatever cursor it committed.
  (with-open [tar (with-retry store :bundle (first keys)
                    #((:bundle-request store) store keys))]
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

(defn- fetch-batch
  "Read a batch whose keys the head has promised exist. A short relaxed read
   is checked with the leader; a short consistent read throws unless `f`
   stopped early. This also applies to a batch containing just one key.

   Only the missing suffix is read again, carrying on from the accumulator
   already produced. Re-reading the whole batch would hand `f` the same events
   twice, and discarding the accumulator would not undo side effects or the
   transients used by `into` and `transduce`.

   End probes are separate: `replay` calls `reduce-bundle` directly for a key
   not yet promised by the head, where an empty answer is legitimate."
  [store keys f init]
  (let [single? (= 1 (count keys))
        first-read (if single? (consistent store) (relaxed store))
        {:keys [acc read stopped?] :as cheap} (reduce-bundle first-read keys f init)
        result (if (or stopped? (= read (count keys)) single?)
                 ;; A singleton has already been asked of the leader.
                 cheap
                 (let [rest-keys (vec (drop read keys))
                       {:keys [acc read stopped?]} (reduce-bundle (consistent store) rest-keys f acc)]
                   {:acc acc
                    :read (+ (long (:read cheap)) (long read))
                    :stopped? stopped?}))]
    (when-not (or (:stopped? result) (= (:read result) (count keys)))
      (throw (ex-info "The stream is missing events the head promised"
                      {:error :missing-event
                       :expected (count keys)
                       :got (:read result)
                       :from (first keys)})))
    result))

(def ^:private max-batch-size
  "The most events one request asks for.

   Tigris accepts fifty times this, and bills a bundle per key rather than per
   request, so a bigger batch buys only fewer round trips. Measured on a real
   bucket, a hundred keys come back in 100ms and two hundred in 183ms, so the
   gain past this is small, while a smaller batch bounds the archive held in
   memory and what a re-read costs.

   Batches do not grow into this. The head bounds every one of them, so a
   replay reads `min` of this and what is left, and the single key it opens
   with already makes reading one event by number cost one request."
  100)

(defn- replay
  "Walk the stream, asking the leader where it ends and everyone else for the
   events themselves.

   Which reads go through the leader is not a matter of taste. Measured against
   a real bucket, a key that is there costs about 1ms relaxed and 6ms through
   the leader, while a key that is *not* there costs about 250ms relaxed and
   9ms through the leader. Reading present events relaxed is seven times
   cheaper; discovering an absent one relaxed is twenty-seven times dearer.

   So a replay never asks a relaxed read about a key that might not exist.

   It opens with one key through the leader. That is the whole of an idle
   replay — a projection asking whether anything happened when nothing has,
   the call that runs most often — and it is also the whole of reading one
   event by number, which is why nothing needs a separate point read.

   Finding something, it spends one LIST on the head and reads up to it in
   batches, relaxed, where every key is known to be there. Passing the head, it
   asks the leader for one more key, which both ends the replay and catches a
   stream that grew while it was being read."
  [{:keys [prefix] :as store} from f init]
  (loop [event-number (long from)
         acc init
         latest nil]
    (cond
      (reduced? acc)
      @acc

      (or (nil? latest)
          (> event-number (long latest)))
      (let [{:keys [acc read stopped?]}
            (reduce-bundle (consistent store) [(event-key prefix event-number)] f acc)]
        (cond
          stopped? @acc
          (zero? (long read)) acc
          :else (recur (inc event-number) acc (head store))))

      :else
      (let [size (min max-batch-size (- (inc (long latest)) event-number))
            keys (bundle-keys prefix event-number (dec (+ event-number size)))
            {:keys [acc read stopped?]} (fetch-batch store keys f acc)]
        (if stopped?
          @acc
          (recur (+ event-number (long read)) acc latest))))))

(defrecord TigrisEventStore [client bucket prefix headers endpoint region
                             credentials-provider http-client bundle-request
                             on-retry]
  event-store/EventAppend
  (try-append! [this event-number event]
    (append! this event-number event))

  event-store/EventSource
  (events [this from]
    (util/reducible
     (fn [rf init]
       (replay this from rf init))))

  event-store/EventHead
  (latest-event-number [this]
    (head this)))

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
   - :on-retry       called with {:op :key :attempt :exception} before every
                     retry of a transient failure, defaults to printing a line
                     to *err*. An outage is otherwise indistinguishable from
                     slowness, so replace this with your own logging."
  [{:keys [bucket prefix access-key-id secret-access-key
           client bundle-request headers on-retry]}]
  (when (str/blank? (str bucket))
    (throw (ex-info "An event store requires :bucket" {:error :incorrect})))
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
                                   (memory-client/tar objects keys))}))

  (doseq [n (range 9)]
    (event-store/try-append! s n {:event/type :example/happened :n n}))

  (event-store/latest-event-number s)

  ;; One event, which costs one request because a batch starts at one:
  (reduce (fn [_ event] (reduced event)) nil (event-store/events s 3))

  ;; A range, in batches that grow to a hundred:
  (into [] (event-store/events s 0))

  ;; And anything a transducer can express, without the store knowing:
  (transduce (filter #(= :example/updated (:event/type %)))
             conj
             []
             (event-store/events s 0))

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

  (first (into [] (event-store/events s 0)))
  (event-store/latest-event-number s)
  ;;=> 0

  (into [] (event-store/events s 0))


  ;; --- a lost append is not an error ---------------------------------------

  (event-store/try-append! s 0 {:event/type :verify/loser})
  ;;=> false. Another writer holds that number; the stored event is untouched.

  (reduce (fn [_ event] (reduced event)) nil (event-store/events s 0))


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
            (reduce (fn [acc event]
                      (-> acc
                          (update :cursor inc)
                          (update :seen conj (:event/type event))))
                    {:cursor -1 :seen #{}}
                    (event-store/events store 0))]
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

  (mapv :event/n (into [] (event-store/events s 0)))
  ;;=> [nil nil 2 3 ... 19], in order

  (into [] (event-store/events s 15))
  ;;=> a replay can start anywhere

  (reduce (fn [acc e] (if (= 3 (count acc)) (reduced acc) (conj acc e)))
          []
          (event-store/events s 0))
  ;;=> reduced stops it, and stops fetching

  (into [] (event-store/events s 99))
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
      (->> (into [] (event-store/events store 0))
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

  (time (reduce (fn [n _] (inc (long n))) 0 (event-store/events big 0)))
  ;;=> one key, one LIST, then batches of a hundred, then the key past the end

  ;; The Tigris console shows what each cost, and which class a bundle
  ;; request bills as.

  )
