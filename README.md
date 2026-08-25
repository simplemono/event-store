# simplemono/event-store

A minimal append-only event log on S3-compatible object storage.

One store is one stream. Everything it owns lives under one prefix:

```
{prefix}/events/{inverted-19d}              one gzip-EDN object per event
{prefix}/packs/{pack-size}/{inverted-19d}   one gzip-EDN vector per :pack-size events
```

Object names use an inverted key-space (`Long/MAX_VALUE - n`, zero-padded to 19
digits), so the newest object sorts first and finding the head is one LIST with
`maxKeys=1`.

## Modules

| module | namespace | depends on |
| --- | --- | --- |
| `core` | `simplemono.event-store` — the `EventStore` protocol | nothing |
| `s3` | `simplemono.event-store.s3` — the S3 implementation, and `…/memory-client` for tests | `core`, `awssdk/s3` |

The protocol namespace has no dependencies of its own, so a future file-,
SQLite- or whatever-backed implementation can satisfy it without pulling in the
AWS SDK.

```clojure
simplemono/event-store-s3 {:git/url "https://github.com/simplemono/event-store.git"
                           :sha "…"
                           :deps/root "s3"}
```

`core` comes along with it; depend on `:deps/root "core"` alone when you only
need the protocol.

## Usage

```clojure
(require '[simplemono.event-store :as event-store]
         '[simplemono.event-store.s3 :as s3])

(def store
  (s3/store
   {:client (s3/client {:endpoint "https://t3.storage.dev"
                        :region "auto"
                        :access-key-id "…"
                        :secret-access-key "…"})
    :bucket "events"
    :prefix "org/acme"
    :headers {"X-Tigris-Consistent" "true"}}))

(event-store/try-append! store 0 {:event/type :example/happened})
;; => true

(event-store/get-event store 0)
;; => {:event/type :example/happened}

(event-store/latest-event-number store)
;; => 0
```

Those three are the whole protocol. `s3/store` builds an implementation of it
and `s3/client` builds an `S3Client`.

## Appending

Event numbers are zero-based and gap-free. `try-append!` is create-only: it
returns `true` when the event was written and `false` when another writer
already took that number. It throws `{:error :gap}` when the previous event is
missing and `{:error :incorrect}` when the number is not a valid one.

The caller chooses the number, which is normally its read-model cursor plus
one. A `false` therefore means the state the caller decided on has moved, and
the right response is to catch the read model up and decide again:

```clojure
(loop []
  (let [cursor (catch-up! read-model)]
    (if-some [event (decide read-model)]
      (or (event-store/try-append! store (inc cursor) event)
          (recur))
      :nothing-to-append)))
```

Deciding inside the loop is what makes this safe: a lost append re-runs the
decision against fresh state instead of replaying a stale one.

An append is one HEAD plus one PUT. The previous event is checked with HEAD
rather than by listing the stream, because LIST is a Class A operation on
object stores such as Tigris while HEAD is Class B — roughly ten times cheaper.

## Failure

A transient failure never reaches the caller. Every request is retried, with
exponential backoff and jitter, until the object store answers: a client-side
exception, a 429 or a 5xx means try again. A 4xx means the request itself is
wrong and is thrown at once, so a bad key or a missing bucket fails loudly
rather than hanging forever. The loop sleeps between attempts, so interrupting
the thread ends it, and `:on-retry` is called before each attempt — replace it
with your own logging, or an outage is indistinguishable from slowness.

Retrying an append is safe because the put is create-only. What a retry cannot
see by itself is whether the attempt that failed had in fact landed: a later
attempt then finds the key taken and cannot tell our own write from somebody
else's. Reading the object back settles it — an equal value was ours.

That is why events must round-trip unchanged, and it is the reason there is no
"the outcome is unknown" result. Resolving an uncertain write is the
implementation's job, because only the implementation knows what it wrote and
where. A `false` from `try-append!` always means somebody else won.

## Packing

Packing is an implementation detail. Completing a range of `:pack-size` events
starts a background thread that writes the packs, and `get-event` reads from a
pack whenever one covers the number. A full replay then costs one request per
thousand events instead of one per event.

Event objects are never deleted, so a pack that is missing, stale or corrupt
only makes reads slower, never wrong.

Packs are namespaced by their size. Changing `:pack-size` on an existing stream
therefore starts a fresh set of packs and re-packs from event 0, instead of
writing new-size packs at indices that already mean something else. The old
packs are orphaned rather than corrupt: they are never read again, and
`packs/{old-size}/` can be deleted whenever convenient.

## Events

Events must be EDN round-trippable values, and should stay small: `:pack-size`
of them end up in one pack object. Keep large payloads in a blob store and put
the blob's name in the event.

## Why single events, not commits

An earlier design of this library put a *commit* in each object — an envelope
holding N events that landed atomically. This one stores a single event per
object. The atomicity commits offer is real, so the trade is worth writing
down.

Legend: 🟢 advantage · 🟡 neutral · 🔴 disadvantage

| # | Dimension | Commits | Single events | Note |
|---|---|---|---|---|
| 1 | Atomicity of N facts *in the log* | 🟢 | 🔴 | Commits genuinely deliver this |
| 2 | Atomicity a consumer can *uphold* | 🔴 | 🟢 | A consumer spanning two stores, or triggering a side effect, cannot honour a commit boundary |
| 3 | Projections across several stores | 🔴 | 🟢 | Follows from row 2 |
| 4 | Projection cursor and resumability | 🟡 | 🟢 | Commit granularity cannot resume mid-commit; event granularity resumes anywhere |
| 5 | Addressing an event | 🔴 | 🟢 | Commits force a compound `(number, index)` through cursors, indexes and logs |
| 6 | Appending N *independent* facts | 🟢 | 🔴 | One conditional PUT versus N |
| 7 | Crash mid-batch | 🟢 | 🔴 | A partial batch is indistinguishable from a deliberate partial |
| 8 | Batching as a use case | 🟡 | 🟢 | A commit boundary carries no domain meaning and is invisible to anything reading events |
| 9 | Pressure to model the domain | 🔴 | 🟢 | Commits make it easy to *avoid* naming the fact that things happened together |
| 10 | Type-filtered replay | 🟡 | 🟢 | A mixed commit can never be skipped; a single event is one keyword test |
| 11 | Entry size and pack density | 🟡 | 🟢 | Batch commits vary in size; single events are uniform, so packs are predictable |
| 12 | Envelope complexity | 🔴 | 🟢 | Two nesting levels, two ids and two timestamps versus one flat map |

**Rows 6 and 7 are the real case for commits, and they are the same case.**
Both describe appending several independent facts at once, and both are
answered by modelling the grouping as a domain event: one
`:order/batch-placed` rather than twelve `:order/placed`. That is one append,
crash-atomic, and the fact that the twelve belong together survives in the log
instead of being an artifact of how the writer happened to batch.

**Row 2 is why it is not close.** A commit promises that N events land
together. A consumer projecting into a single SQLite file can honour that. A
consumer that writes two stores, or sends an email, or calls an API, cannot. An
atomicity guarantee that some consumers are structurally unable to uphold is
worse than no guarantee, because someone will eventually rely on it and be
right to.

The rule that follows is: **if two facts must be true together, they must be
one event.** What this genuinely costs is facts that must be atomic and cannot
be merged into one event. Those belong in a transactional store, not in this
log.

## Options

`s3/store` requires `:client`, `:bucket` and `:prefix`, and accepts:

| option | default | meaning |
| --- | --- | --- |
| `:headers` | `{}` | extra request headers |
| `:pack-size` | `1000` | events per pack |
| `:pack?` | `true` | set false to never pack |
| `:pack-async?` | `true` | set false to pack on the appending thread |
| `:on-pack-error` | prints to `*err*` | called with the Throwable when background packing fails |
| `:on-retry` | prints to `*err*` | called with `{:op :key :attempt :exception}` before each retry |

## Testing

`simplemono.event-store.memory-client/client` is an in-memory `S3Client`. It is
a fake transport rather than a second storage backend, so a test exercises the
real code: the same key encoding, the same inverted ordering, the same gzip,
the same create-only put and the same packing. Only the network is missing.

```clojure
(require '[simplemono.event-store.memory-client :as memory-client])

(s3/store {:client (memory-client/client)
           :bucket "events"
           :prefix "org/acme"
           :pack-size 4
           :pack-async? false})
```

Pass your own atom to `client` to inspect the stored objects.

Run the tests with `cd s3 && clojure -M:test`.

## License

MIT
