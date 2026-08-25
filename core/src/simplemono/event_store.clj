(ns simplemono.event-store
  "The event store protocol.

   An event store is one stream: an append-only sequence of events numbered
   from zero, without gaps. Implementations own their storage and their
   optimizations; this namespace owns nothing but the contract, and has no
   dependencies of its own so that a backend can implement it without dragging
   in another backend's.

   `simplemono.event-store.s3` is the implementation for S3-compatible object
   storage.

   Command handling, projections, retries and idempotency live in the
   application. The usual loop is to catch a read model up, decide against it,
   and append at the cursor plus one; a false return means the state the
   decision rested on has moved, so the caller catches up and decides again.")

(defprotocol EventStore
  (try-append! [store event-number event]
    "Create-only append of `event` at zero-based `event-number`.

     Returns true when the event was written, and false when `event-number`
     already exists — another writer won the race.

     Throws `ex-info` with `:error` in its `ex-data` for exceptional states:

       :incorrect  `event-number` is not a valid event number
       :gap        appending here would leave a hole, because the previous
                   event does not exist
       :ambiguous  the append may or may not have landed and the
                   implementation could not determine which

     An event must be a value the implementation can store and read back
     unchanged; equality of the round trip is what makes :ambiguous
     resolvable.")

  (get-event [store event-number]
    "The event at `event-number`, or nil when it does not exist.")

  (latest-event-number [store]
    "The highest event number in the stream, or nil when the stream is empty."))
