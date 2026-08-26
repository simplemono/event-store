(ns simplemono.event-store.memory-client
  "An in-memory S3Client, for tests.

   It is a fake transport rather than a second storage backend, so a test runs
   the real `simplemono.event-store` code: the same key encoding, the same
   inverted ordering, the same gzip, the same create-only put and the same tar
   parsing. Only the network is missing.

   It implements exactly the four operations the event store performs —
   putObject with If-None-Match, getObject, headObject and a prefix listing
   with maxKeys — and nothing else. `tar` stands in for the Tigris bundle API,
   which is not an S3 operation at all."
  (:require [clojure.string :as str])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream)
           (software.amazon.awssdk.core ResponseInputStream)
           (software.amazon.awssdk.core.sync RequestBody)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model GetObjectRequest
                                                     GetObjectResponse
                                                     HeadObjectRequest
                                                     HeadObjectResponse
                                                     ListObjectsV2Request
                                                     ListObjectsV2Response
                                                     NoSuchKeyException
                                                     PutObjectRequest
                                                     PutObjectResponse
                                                     S3Exception
                                                     S3Object)))

(defn- no-such-key
  []
  (-> (NoSuchKeyException/builder)
      (.statusCode 404)
      (.message "No such key")
      (.build)))

(defn- precondition-failed
  []
  (-> (S3Exception/builder)
      (.statusCode 412)
      (.message "Precondition Failed")
      (.build)))

(defn- request-bytes
  [^RequestBody body]
  (with-open [in (.newStream (.contentStreamProvider body))]
    (let [out (ByteArrayOutputStream.)]
      (.transferTo in out)
      (.toByteArray out))))

(defn- request-headers
  [request]
  (some-> request .overrideConfiguration (.orElse nil) .headers))

(defn- create-only?
  [request]
  (boolean (some #{"*"} (get (request-headers request) "If-None-Match"))))

(defn client
  "An S3Client backed by `objects`, an atom holding a sorted map of key to
   byte array. Pass your own atom to inspect or seed the stored objects;
   the zero-arity creates a fresh one."
  ([]
   (client (atom (sorted-map))))
  ([objects]
   (reify S3Client
     (^PutObjectResponse putObject [_ ^PutObjectRequest request ^RequestBody body]
       (let [key (.key request)]
         (when (and (create-only? request)
                    (contains? @objects key))
           (throw (precondition-failed)))
         (swap! objects assoc key (request-bytes body))
         (-> (PutObjectResponse/builder) (.build))))

     (^ResponseInputStream getObject [_ ^GetObjectRequest request]
       (if-let [bytes (get @objects (.key request))]
         (ResponseInputStream. (-> (GetObjectResponse/builder) (.build))
                               (ByteArrayInputStream. bytes))
         (throw (no-such-key))))

     (^HeadObjectResponse headObject [_ ^HeadObjectRequest request]
       (if (contains? @objects (.key request))
         (-> (HeadObjectResponse/builder) (.build))
         (throw (no-such-key))))

     (^ListObjectsV2Response listObjectsV2 [_ ^ListObjectsV2Request request]
       (let [prefix (or (.prefix request) "")
             matching (->> @objects
                           keys
                           sort
                           (filter #(str/starts-with? % prefix)))
             max-keys (or (.maxKeys request) (count matching))
             contents (->> matching
                           (take max-keys)
                           (mapv #(-> (S3Object/builder) (.key %) (.build))))]
         (-> (ListObjectsV2Response/builder)
             (.contents contents)
             (.isTruncated (boolean (> (count matching) (count contents))))
             (.build)))))))

(def ^:private block-size 512)

(defn- octal-field!
  [^bytes block offset width n]
  (let [s (str (format (str "%0" (dec (long width)) "o") (long n)) (char 0))]
    (System/arraycopy (.getBytes s "UTF-8") 0 block offset (count s))))

(defn- tar-header
  [^String name size]
  (let [block (byte-array block-size)
        name-bytes (.getBytes name "UTF-8")]
    (when (< 100 (alength name-bytes))
      (throw (ex-info "Key too long for a plain tar header" {:name name})))
    (System/arraycopy name-bytes 0 block 0 (alength name-bytes))
    (octal-field! block 100 8 420)
    (octal-field! block 108 8 0)
    (octal-field! block 116 8 0)
    (octal-field! block 124 12 size)
    (octal-field! block 136 12 0)
    (aset-byte block 156 (byte (int \0)))
    (System/arraycopy (.getBytes "ustar 00" "UTF-8") 0 block 257 8)
    ;; The checksum is computed with its own field read as spaces.
    (java.util.Arrays/fill block 148 156 (byte (int \space)))
    (octal-field! block 148 8 (reduce + (map #(bit-and (long %) 255) block)))
    (aset-byte block 154 (byte 0))
    (aset-byte block 155 (byte (int \space)))
    block))

(defn tar
  "A tar of the objects at `keys`, in that order, as an InputStream.

   Mimics the Tigris bundle API closely enough to exercise a real tar reader:
   entries are named by their full key, and a key with no object is left out,
   the way `x-tigris-bundle-on-error: skip` behaves."
  [objects keys]
  (let [out (ByteArrayOutputStream.)]
    (doseq [key keys
            :let [^bytes content (get @objects key)]
            :when content]
      (.write out ^bytes (tar-header key (alength content)))
      (.write out content)
      (let [padding (mod (- block-size (mod (alength content) block-size)) block-size)]
        (when (pos? padding)
          (.write out (byte-array padding)))))
    (.write out (byte-array (* 2 block-size)))
    (ByteArrayInputStream. (.toByteArray out))))
