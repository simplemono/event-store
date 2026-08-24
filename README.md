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

## Usage

```clojure
(require '[simplemono.event-store :as event-store])

(def store
  (event-store/store
   {:client (event-store/client {:endpoint "https://t3.storage.dev"
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

That is the whole API, plus `client` for building an `S3Client`.

## Appending

Event numbers are zero-based and gap-free. `try-append!` is create-only: it
returns `true` when the event was written and `false` when another writer
already took that number. It throws `{:error :gap}` when the previous event is
missing and `{:error :ambiguous}` when the outcome could not be determined.

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

An ambiguous PUT is resolved by reading the object back: our own event means
the put landed, a different event means another writer won, nothing at all
means the outcome is genuinely unknown. No event field is required for this, so
the library stays agnostic about event shape.

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

## Options

`store` requires `:client`, `:bucket` and `:prefix`, and accepts:

| option | default | meaning |
| --- | --- | --- |
| `:headers` | `{}` | extra request headers |
| `:pack-size` | `1000` | events per pack |
| `:pack?` | `true` | set false to never pack |
| `:pack-async?` | `true` | set false to pack on the appending thread |
| `:on-pack-error` | prints to `*err*` | called with the Throwable when background packing fails |

## Testing

`simplemono.event-store.memory-client/client` is an in-memory `S3Client`. It is
a fake transport rather than a second storage backend, so a test exercises the
real code: the same key encoding, the same inverted ordering, the same gzip,
the same create-only put and the same packing. Only the network is missing.

```clojure
(require '[simplemono.event-store.memory-client :as memory-client])

(event-store/store {:client (memory-client/client)
                    :bucket "events"
                    :prefix "org/acme"
                    :pack-size 4
                    :pack-async? false})
```

Pass your own atom to `client` to inspect the stored objects.

Run the tests with `clojure -M:test`.

## License

MIT
