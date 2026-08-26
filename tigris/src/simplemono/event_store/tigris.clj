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
   [acc read].

   Entry names are checked against the keys, because a gap-free stream cannot
   legitimately skip one and a replay that quietly dropped an event would be
   far worse than one that stopped. Tigris leaves a key with no object out of
   the archive rather than failing, so `read` also reports how far the batch
   actually got."
  [store keys f init]
  (with-open [tar ((:bundle-request store) store keys)]
    (let [result
          (bundle/reduce-tar
           tar
           (fn [[acc read] [name ^bytes content]]
             (if (= "__bundle_errors.json" name)
               (reduced [acc read])
               (let [expected (nth keys read nil)]
                 (when-not (= expected name)
                   (throw (ex-info "Bundle returned an unexpected object"
                                   {:error :missing-event
                                    :expected expected
                                    :got name})))
                 (let [acc (f acc (decode content))]
                   (if (reduced? acc)
                     (reduced [acc (inc (long read))])
                     [acc (inc (long read))])))))
           [init 0])]
      result)))

(defn- replay
  "Walk the stream in bundles, never asking for a key that cannot exist.

   The head bounds every batch. Asking for a full batch and letting Tigris skip
   what is missing would also work, and is how an earlier version found the end
   of a stream — but it makes the last request of every replay ask for
   thousands of absent objects, which is wasteful at best. One LIST for the
   head is cheaper than that, and it is re-read once at the end in case the
   stream grew while we were reading it."
  [{:keys [prefix bundle-size] :as store} from f init]
  (loop [event-number (long from)
         acc init
         latest (head store)]
    (cond
      (reduced? acc)
      @acc

      (or (nil? latest)
          (> event-number (long latest)))
      (let [grown (head store)]
        (if (and grown (> (long grown) (long (or latest -1))))
          (recur event-number acc grown)
          acc))

      :else
      (let [to (min (long latest) (dec (+ event-number (long bundle-size))))
            keys (bundle-keys prefix event-number to)
            [acc read] (reduce-bundle store keys f acc)]
        (if (< (long read) (count keys))
          ;; Either `f` stopped early, or an event the head promised was not
          ;; there — both mean this traversal is over.
          (if (reduced? acc) @acc acc)
          (recur (+ event-number (long read)) acc latest))))))

(defrecord TigrisEventStore [client bucket prefix headers endpoint region
                             credentials-provider http-client bundle-request
                             bundle-size on-retry]
  event-store/EventStore
  (try-append! [this event-number event]
    (append! this event-number event))

  (get-event [this event-number]
    (event-object this event-number))

  (latest-event-number [this]
    (head this))

  event-store/EventReplay
  (-reduce-events [this from f init]
    (replay this from f init)))

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
   - :on-retry       called with {:op :key :attempt :exception} before every
                     retry of a transient failure, defaults to printing a line
                     to *err*. An outage is otherwise indistinguishable from
                     slowness, so replace this with your own logging."
  [{:keys [bucket prefix access-key-id secret-access-key
           client bundle-request headers bundle-size on-retry]
    :or {bundle-size bundle/max-keys}}]
  (when (str/blank? (str bucket))
    (throw (ex-info "An event store requires :bucket" {:error :incorrect})))
  (when-not (and (pos-int? bundle-size)
                 (<= bundle-size bundle/max-keys))
    (throw (ex-info ":bundle-size must be between 1 and the Tigris maximum"
                    {:error :incorrect
                     :bundle-size bundle-size
                     :maximum bundle/max-keys})))
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
