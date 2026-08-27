# simplemono/event-store

An append-only event log on [Tigris](https://www.tigrisdata.com/).

It is built for the projects it is used in rather than for everyone. It targets
Tigris specifically, and leans on two things Tigris gives you that plain S3 does
not: strong consistency on demand, and an API that returns thousands of objects
in one request.

One store is one stream, under one prefix in one bucket:

```
{prefix}/events/{inverted-19d}   one gzip-EDN object per event
```

That is the entire layout. There is nothing else to build, keep current or
clean up.

Object names use an inverted key-space (`Long/MAX_VALUE - n`, zero-padded to 19
digits), so the newest object sorts first and finding the head is one LIST with
`maxKeys=1`.

## Modules

| module | namespace | depends on |
| --- | --- | --- |
| `core` | `simplemono.event-store` — the `EventAppend` and `EventSource` protocols | nothing |
| `tigris` | `simplemono.event-store.tigris` — the implementation | `core`, `awssdk/s3`, `commons-compress` |
| `memory` | `simplemono.event-store.memory` — an in-memory implementation | `core` |
| `memory-client` | `simplemono.event-store.memory-client` — test doubles | `awssdk/s3`, `commons-compress` |

The protocol namespace has no dependencies of its own, so another
implementation can satisfy it without pulling in the AWS SDK — `memory` is the
worked example.

```clojure
simplemono/event-store-tigris {:git/url "https://github.com/simplemono/event-store.git"
                               :sha "…"
                               :deps/root "tigris"}
```

`core` comes along with it; depend on `:deps/root "core"` alone when you only
need the protocol, and on `memory` in your tests.

## Usage

```clojure
(require '[simplemono.event-store :as event-store]
         '[simplemono.event-store.tigris :as tigris])

(def store
  (tigris/store {:bucket "events"
                 :prefix "org/acme"
                 :access-key-id "…"
                 :secret-access-key "…"}))

(event-store/try-append! store 0 {:event/type :example/happened})
;; => true

(reduce f init (event-store/events store 0))
(into [] (event-store/events store 0))
(transduce (filter interesting?) conj [] (event-store/events store 42))
```

That is the whole interface. Two protocols with one method each, because
reading and writing a stream are separate jobs, and a projection can be handed
something that only reads.

`events` returns something reducible and deliberately not seqable. The
implementation holds an archive open while it walks, and reducing means that is
closed by the time the call returns, which a lazy sequence handed to a caller
could not promise.

There is no separate point read. Reading one event by number is a reduction
that stops after the first, and costs one request:

```clojure
(reduce (fn [_ event] (reduced event)) nil (event-store/events store 42))
```

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

## Replaying

**A replay does not read one object per event.** Tigris can return many objects
as a single streaming tar — `POST /{bucket}?bundle` with a list of keys — so
reading a stream costs about one request per hundred events rather than one per
event.

Nothing is written to make this fast. There are no packs, no index, no
compaction, nothing to keep current and nothing to rebuild after a schema
change, and the first replay of a stream is exactly as cheap as the tenth.

A replay has three steps.

**One key, through the leader.** This is the whole of an idle replay, a
projection asking whether anything happened when nothing has, which is the call
that runs most often. It is also the whole of reading one event by number. One
request, no LIST.

**One LIST for the head**, and only once there is something to read. It is a
Class A operation, about ten times the price of a read, and it earns that by
bounding every batch that follows.

**Batches up to the head, relaxed.** Every key in them is known to be there.
Passing the head, one more key is asked of the leader, which ends the replay
and would catch a stream that grew while it was being read.

### Which reads go through the leader

Not a matter of taste. Measured against a real bucket:

| | relaxed | through the leader |
| --- | --- | --- |
| a key that is there | ~1ms | ~6ms |
| a key that is **not** there | **~250ms** | ~9ms |

Reading present events relaxed is seven times cheaper. Discovering an absent
one relaxed is twenty-seven times dearer. So a replay never asks a relaxed read
about a key that might not exist, which is what the head bound is for, and it
asks single keys of the leader, where a miss is cheap.

That is also why a full batch past the end of a stream is not an option, though
it would be simpler: a hundred absent keys is twenty-five seconds, and a
thousand is four minutes. Setting `on-error` to fail rather than skip does not
help, because Tigris resolves every key before it decides.

### What a short batch means

Usually a replica that has not caught up. The batch is bounded by the head, so
those events were promised, and stopping there would silently truncate the
replay. Only the keys the first read did not return are asked again, of the
leader, carrying on from the accumulator the first read produced. Re-reading
the whole batch would hand the reducing function the same events twice, which
`into` and `transduce` would not undo, because they use transients.

Entry names are checked against the keys asked for. A gap-free stream cannot
legitimately skip one, so an event arriving where another was expected stops
the replay. The exception is a missing key at the *end* of a batch, which
cannot be told from the end of the stream. Nothing ever deletes an event, so
that means corruption rather than a race.

### Measured

Two hundred events on a real bucket: **four bundle requests and one LIST, about
350ms**. An idle replay is one request. Reading one event by number is one
request.

## Events

Events must be EDN round-trippable values, and should stay small: a replay
pulls up to a hundred of them in one response. Keep large payloads in a blob
store and put the blob's name in the event.

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
| 11 | Entry size | 🟡 | 🟢 | Batch commits vary in size; single events are uniform, so a bundle's size is predictable |
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

`tigris/store` requires `:bucket` and `:prefix`, plus credentials unless the
default provider chain has them, and accepts:

| option | default | meaning |
| --- | --- | --- |
| `:client` | built from the credentials | an `S3Client`, if you would rather build it or hand in a double |
| `:bundle-request` | the real bundle POST | a fn of `[store keys]` returning a tar `InputStream`, for tests |
| `:headers` | `X-Tigris-Consistent: true` | extra request headers, merged over the default |
| `:on-retry` | prints to `*err*` | called with `{:op :key :attempt :exception}` before each retry |

There is no batch-size option. Batches are bounded by the head and capped at a
hundred, which is where the round-trip saving flattens out: a hundred keys come
back in 100ms and two hundred in 183ms.

`tigris/latest-event-number` is a plain function rather than a protocol method.
A replay finds the end of a stream by walking off it and an append is told its
number by the caller, so nothing in the library needs to dispatch on it. It is
there for a health check or a look at a stream from the REPL.

## Testing your own code

Depend on `memory` and build a store that needs no network:

```clojure
(require '[simplemono.event-store :as event-store]
         '[simplemono.event-store.memory :as memory])

(def store (memory/store))

(event-store/try-append! store 0 {:event/type :example/happened})
(into [] (event-store/events store 0))
```

Pass your own atom over a sorted map to `memory/store` to seed a stream or to
inspect one. Appends are serialised, so concurrent writers see the same
create-only, gap-free behaviour Tigris gives them. Reading one event from a map
costs what reading a hundred does, so `events` there is the generic walk in
`simplemono.event-store.util/one-at-a-time`, which any implementation can use in
a line.

What it cannot reproduce is a network: there is no retrying and no uncertain
write, because an append here either happened or threw.

## Testing this library

```
cd memory && clojure -M:test
cd tigris && clojure -M:test
```

The `tigris` suite runs against `memory-client`, which fakes the two transports
this library uses: an in-memory `S3Client`, and a `tar` function standing in for
the bundle API. They are fakes of the transport, not of the store, so the suite
exercises the real code — the same key encoding, inverted ordering, gzip,
create-only put, retrying and tar parsing — with only the network missing.

The fake writes its archives with Commons Compress in POSIX long-file mode, so
a long key becomes a pax extended header there as it does on Tigris. It is not
an exact mimic: it reaches for pax at 100 characters where Tigris keeps
splitting into the ustar name prefix until 256, so the suite covers plain ustar
and pax while Tigris's middle case needs a real bucket.

Nothing depends on `memory-client` at runtime; it is a `:test` dependency.

## License

MIT
