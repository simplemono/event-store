(ns simplemono.event-store.util
  "Helpers for implementing `simplemono.event-store/events`.

   Nothing here is part of the contract. It exists so an implementation can say
   what it does without repeating the interop, and so the protocol namespace
   holds the protocols and nothing else."
  (:import (clojure.lang IReduceInit)))

(defn reducible
  "Wraps `f`, a function of a reducing function and an initial value, as
   something `reduce` accepts."
  [f]
  (reify IReduceInit
    (reduce [_ rf init]
      (f rf init))))

(defn one-at-a-time
  "An `events` implementation for storage with no bulk read.

   Fetches events one by one with `read-event`, a function of an event number
   returning the event or nil. Correct anywhere, and the right thing where
   reading a hundred events costs what reading one does."
  [read-event from]
  (reducible
   (fn [rf init]
     (loop [event-number (long from)
            acc init]
       (if (reduced? acc)
         @acc
         (if-some [event (read-event event-number)]
           (recur (inc event-number) (rf acc event))
           acc))))))
