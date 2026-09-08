(ns simplemono.event-store
  "The event store protocols.

   An event store is one stream: an append-only sequence of events numbered
   from zero, without gaps. Implementations own their storage and their
   optimizations; this namespace owns nothing but the contract, and has no
   dependencies of its own so that a backend can implement it without dragging
   in another backend's.

   `simplemono.event-store.tigris` is the implementation for Tigris.

   There are three protocols with one method each, because appending to a
   stream, reading it, and asking where it ends are separate jobs. A
   projection can be handed something that only reads and cannot append by
   mistake, and a consumer of `EventSource` alone never has to implement a
   head lookup it does not need.

   This namespace holds the protocols and nothing else.
   `simplemono.event-store.util` has the helpers an implementation of `events`
   would otherwise repeat.

   Command handling, projections, retries and idempotency live in the
   application. The usual loop is to catch a read model up, decide against it,
   and append at the cursor plus one; a false return means the state the
   decision rested on has moved, so the caller catches up and decides again.")

(defprotocol EventAppend
  (try-append! [store event-number event]
    "Create-only append of `event` at zero-based `event-number`.

     `event-number` must be a non-negative java.lang.Long. Other numeric types
     are not coerced. Long/MAX_VALUE is the last addressable position.

     Returns true when this invocation wrote the event, including its retries,
     and false when another invocation owns `event-number`, even if the event
     values are equal.

     Throws `ex-info` with `:error` in its `ex-data` for exceptional states:

       :incorrect  `event-number` is not a valid event number
       :gap        appending here would leave a hole, because the previous
                   event does not exist

     Beyond those, an implementation may throw whatever its storage throws.
     Only a normal true/false return guarantees a resolved append outcome.
     An exceptional exit, including interruption or cancellation, may occur
     after the event was written. Cancellation is propagated, not retried or
     converted to false. Catch up application state before deciding what to do
     after an exceptional append; do not assume that nothing was written.

     An event must be a value the implementation can store and read back
     unchanged. Write ownership is separate from event equality."))

(defprotocol EventSource
  (events [store from]
    "The events from `from` onwards, as something `reduce` can walk.

       (reduce f init (events store 0))
       (transduce (filter interesting?) conj [] (events store 42))

     `from` must be a non-negative java.lang.Long, just like an append position.
     Invalid positions throw ex-info with {:error :incorrect} when `events` is
     called, before any reading or reduction begins.

     The walk stops at the first event number that does not exist or after
     Long/MAX_VALUE, and `f` may return `reduced` to stop sooner. Stored nil and
     false are events, not end-of-stream markers.

     What comes back is reducible and deliberately not seqable. An
     implementation may hold a connection or an archive open while it reads,
     and reducing means that is closed by the time the call returns, which a
     lazy sequence handed to a caller could not promise. Anyone who wants the
     whole stream in memory can still write `(into [] …)` and say so.

     Failures while consuming an open stream, decoding events, or running `f`
     propagate. A failed walk may already have delivered a prefix to `f`; those
     effects are not rolled back or replayed automatically. The caller resumes from its last durably committed cursor,
     which need not be the last event delivered. Commit projection updates and
     their cursor together; external effects need their own idempotency.

     How the events are fetched is the store's business, because only the store
     knows what a request costs."))

(defprotocol EventHead
  (latest-event-number [store]
    "The highest event number in the stream as a java.lang.Long, or nil when empty.

     Nothing in this library needs it: a replay finds the end of a stream by
     walking off it, and an append is told its number by the caller, normally
     a read-model cursor plus one. It is a protocol method because callers
     do need it — deciding without a read model, a health check, a look at a
     stream from the REPL — and generic code should not have to know which
     implementation it holds to ask.

     It is deliberately not part of `EventSource`: reading by reducing never
     requires the head, so something that only implements `events` stays a
     one-method reify. What an answer costs is the implementation's business —
     on Tigris it is one LIST, about ten times the price of a read."))
