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
           (org.apache.commons.compress.archivers.tar TarArchiveEntry
                                                      TarArchiveOutputStream)
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

(defn tar
  "A tar of the objects at `keys`, in that order, as an InputStream.

   Mimics the Tigris bundle API closely enough to exercise a real tar reader.
   A key with no object is left out, the way `x-tigris-bundle-on-error: skip`
   behaves. Long-file mode is POSIX because that is what Tigris does: a key
   that fits is a plain ustar entry and a longer one becomes a pax extended
   header, so a reader that only understands ustar fails here as it would
   there."
  [objects keys]
  (let [bytes (ByteArrayOutputStream.)]
    (with-open [out (TarArchiveOutputStream. bytes)]
      (.setLongFileMode out TarArchiveOutputStream/LONGFILE_POSIX)
      (doseq [key keys
              :let [^bytes content (get @objects key)]
              :when content]
        (let [entry (TarArchiveEntry. ^String key)]
          (.setSize entry (alength content))
          (.putArchiveEntry out entry)
          (.write out content)
          (.closeArchiveEntry out))))
    (ByteArrayInputStream. (.toByteArray bytes))))
