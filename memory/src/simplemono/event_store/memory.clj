(ns simplemono.event-store.memory
  "`simplemono.event-store/EventAppend`, `EventSource` and `EventHead` in
   memory.

   One store is one stream, held in an atom over a sorted map from event
   number to event. Nothing is persisted and nothing is shared between stores.

   It exists so that code built on the protocol — command loops, projections,
   read models — can be tested without a network, and so that the protocol has
   a second implementation keeping it honest.

   Appends are serialised with `locking`, so concurrent writers see the same
   create-only, gap-free behaviour an object store gives them. Unlike a store
   reached over a network, an append here either happened or threw, so there is
   never an uncertain write to resolve."
  (:require [simplemono.event-store :as event-store]
            [simplemono.event-store.util :as util]))

(defn- gap!
  [event-number expected]
  (throw (ex-info "Append would create a gap"
                  {:error :gap
                   :event-number event-number
                   :expected expected})))

(defrecord MemoryEventStore [state]
  event-store/EventAppend
  (try-append! [_ event-number event]
    (util/check-event-number! event-number)
    (locking state
      (let [events @state
            latest (last (keys events))
            expected (when-not (= latest Long/MAX_VALUE)
                       (if (some? latest) (inc (long latest)) 0))]
        (cond
          (contains? events event-number)
          false

          (not= event-number expected)
          (gap! event-number expected)

          :else
          (do
            (swap! state assoc event-number event)
            true)))))

  event-store/EventSource
  (events [_ from]
    ;; Reading one event from a map costs what reading a hundred does, so the
    ;; generic walk is also the fastest one available here.
    (util/one-at-a-time #(find @state %) from))

  event-store/EventHead
  (latest-event-number [_]
    (last (keys @state))))

(defn store
  "An in-memory event store.

   Pass your own atom over a sorted map to seed the stream or to inspect it;
   the zero-arity creates a fresh one."
  ([]
   (store (atom (sorted-map))))
  ([state]
   (->MemoryEventStore state)))

(comment

  (def s (store))

  (event-store/latest-event-number s)

  (event-store/try-append! s 0 {:event/type :example/created})
  (event-store/try-append! s 0 {:event/type :example/created})

  (into [] (event-store/events s 0))
  (event-store/latest-event-number s)

  ;; Seed a stream and look at it:
  (def state (atom (sorted-map)))
  (def seeded (store state))
  (event-store/try-append! seeded 0 {:event/type :example/created})
  @state

  )
