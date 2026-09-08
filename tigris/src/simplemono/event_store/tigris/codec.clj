(ns simplemono.event-store.tigris.codec
  "Internal event payload codec: plain data in regular, self-describing Nippy
   frames. No Java serialization, reader fallback, or caller-configured codec
   behavior. Changing this format is a storage compatibility decision."
  (:require [taoensso.nippy :as nippy]))

(def ^:private scalar-types
  #{Boolean Character String clojure.lang.Keyword clojure.lang.Symbol
    Byte Short Integer Long Float Double clojure.lang.BigInt clojure.lang.Ratio
    java.math.BigInteger java.math.BigDecimal java.util.UUID java.util.Date
    java.time.Instant (class (byte-array 0))})

(defn- unsupported!
  [value]
  (throw (ex-info "Unsupported event value"
                  {:error :incorrect :type (some-> value class .getName)})))

(defn- check-value!
  [value]
  ;; Nippy also natively supports records and other JVM objects. Those are
  ;; deliberately outside the event contract, even if they can be frozen.
  (cond
    (or (nil? value) (contains? scalar-types (class value))) nil
    (or (record? value) (instance? clojure.lang.IType value)) (unsupported! value)
    (map? value) (doseq [[k v] value] (check-value! k) (check-value! v))
    (or (vector? value) (set? value) (seq? value)) (run! check-value! value)
    :else (unsupported! value))
  ;; Nippy reconstructs sorted collections with the default comparator.
  (when (and (sorted? value)
             (not (identical? (.comparator ^clojure.lang.Sorted value)
                              clojure.lang.RT/DEFAULT_COMPARATOR)))
    (unsupported! value))
  (when-let [metadata (meta value)]
    (check-value! metadata)))

(defn encode
  "Validate plain data, then freeze once before any PUT. No round-trip decode
   is needed on the write path; byte arrays need not satisfy Clojure's `=`."
  [event]
  (check-value! event)
  (nippy/freeze event
                {:compressor :auto
                 :auto-freeze-compressor nil
                 :freeze-fallback (fn [_ value] (unsupported! value))
                 :serializable-allowlist #{}
                 :incl-metadata? true
                 :shared-dict nil}))

(defn decode
  "Read a regular Nippy frame, using its header to select decompression.
   No legacy gzip-EDN or headerless frames are supported."
  [bytes]
  (binding [nippy/*serializable-whitelist* nil
            *data-readers* {}
            *default-data-reader-fn* nil]
    (let [event (nippy/thaw bytes
                            {:compressor :auto
                             ;; Throw rather than returning a quarantine placeholder.
                             :serializable-allowlist
                             (fn [class-name]
                               (throw (ex-info "Java-serialized events are not supported"
                                               {:error :invalid-event :class-name class-name})))
                             :custom-readers {}
                             :incl-metadata? true
                             :shared-dict nil
                             :thaw-xform nil})]
      (check-value! event)
      event)))
