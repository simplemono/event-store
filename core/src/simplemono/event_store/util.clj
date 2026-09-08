(ns simplemono.event-store.util
  "Helpers for implementing the event store protocols.

   Nothing here is part of the contract. It exists so an implementation can say
   what it does without repeating the interop, and so the protocol namespace
   holds the protocols and nothing else."
  (:import (clojure.lang IReduceInit)))

(defn check-event-number!
  "Reject anything other than a non-negative java.lang.Long, without coercion."
  [event-number]
  (when-not (and (instance? Long event-number)
                 (not (neg? (long event-number))))
    (throw (ex-info "Event numbers must be non-negative java.lang.Long values"
                    {:error :incorrect :event-number event-number}))))

(defn reducible
  "Wraps `f`, a function of a reducing function and an initial value, as
   something `reduce` accepts."
  [f]
  (reify IReduceInit
    (reduce [_ rf init]
      (f rf init))))

(defn one-at-a-time
  "An `events` implementation for storage with no bulk read.

   Fetches events one by one with `find-event`, a function of an event number
   returning a map entry (as from `find`), or nil when absent. The entry's value
   is the event, so a stored nil is distinct from absence. Reading a hundred
   events here costs what reading one does."
  [find-event from]
  (check-event-number! from)
  (reducible
   (fn [rf init]
     (loop [event-number (long from)
            acc init]
       (if (reduced? acc)
         @acc
         (if-some [entry (find-event event-number)]
           (let [acc (rf acc (val entry))]
             (if (or (reduced? acc) (= event-number Long/MAX_VALUE))
               (unreduced acc)
               (recur (inc event-number) acc)))
           acc))))))
