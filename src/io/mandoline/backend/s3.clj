(ns io.mandoline.backend.s3
  (:require
    [clojure.set :refer [difference]]
    [clojure.string :as string]
    [clojure.tools.logging :as log]
    [amazonica.aws.s3 :as s3]
    [io.mandoline.utils :as utils]
    [io.mandoline.impl.protocol :as proto])
  (:import
    [java.net URI]
    [java.util UUID]
    [java.io ByteArrayInputStream]
    [java.nio ByteBuffer]
    [javax.net.ssl SSLPeerUnverifiedException]
    [org.joda.time DateTime]
    [com.amazonaws AmazonClientException AmazonServiceException]
    [com.amazonaws.auth
     DefaultAWSCredentialsProviderChain
     STSAssumeRoleSessionCredentialsProvider]))

; TODO: integrate this functionality with Mandoline utils/attest.
(defmacro assertex
  "Replacement from assert that throws IllegalArgumentException
  and includes the form in the error message."
  [form msg]
  (let [pred (first form)
        args (rest form)]
    `(let [result# ~form]
       (when-not result#
         (throw (IllegalArgumentException.
                  (str ~msg "\nfailed: " (cons '~pred (list ~@args))))))
       result#)))

; String service functions
(defn- last-segment
  [k]
  (if (nil? k)
    nil
    (last (string/split k #"/"))))

(defn- valid-version-id?
  [version-id]
  (and (not (nil? version-id))
       (not (.contains (str version-id) "/"))))

(defn- str->timestamp
  [str]
  (try
    (-> str Long/parseLong DateTime.)
    (catch NumberFormatException e nil)))

; S3 keys used by different pieces of the backend.
(defn- dataset-registry-dir
  [key-prefix]
  (string/join "/" [key-prefix "db-dataset-registry"]))

(defn- dataset-marker-key
  "Returns the key name for the marker that a dataset exists."
  [key-prefix dataset]
  (string/join "/" [(dataset-registry-dir key-prefix) dataset]))

(defn- version-registry-dir
  "Returns the directory that stores the list of versions."
  [key-prefix dataset]
  (string/join "/" [key-prefix dataset "versions"]))

(defn- version-key
  "Returns the key for a version marker in the version index.
  hex-code: the internal version ID generated for this version
    (oldest version is 0xFFFFFFFF, counts down from there)
  version-id: the external version id."
  [key-prefix dataset hex-code version-id]
  (string/join "/" [(version-registry-dir key-prefix dataset)
                    (format "%08x" hex-code)
                    version-id]))

(defn- chunk-key
  "Returns the key name for a particular chunk object."
  [key-prefix dataset hash]
  (string/join "/" [key-prefix dataset "chunks" hash]))

(defn- index-dir
  "Returns the directory name in the index for a particular variable at coord."
  [key-prefix dataset var-name coord version-id]
  (assertex (vector? coord) "coord must be a vec")
  (assertex (valid-version-id? version-id)
            "invalid version-id")
  (string/join "/" (list* key-prefix dataset "indices"
                          (name var-name) (conj coord version-id))))

(defn- index-key-end
  "Returns the last part of the index key."
  [hash old-hash old-timestamp]
  (string/join "." [hash old-hash old-timestamp]))

(defn- split-index-key-end
  "Splits the last part of the index key and returns
  [hash old-hash old-timestamp]."
  [k]
  (string/split k #"\."))

(defn- hex-code-from-version-key
  "Takes '/.../versions/A7BB13/version-id'
  and extracts A7BB13 as a number"
  [key]
  (-> key
      (string/split #"/")
      drop-last
      last
      (Integer/parseInt 16)))

; S3 access methods
(defn s3-retry
  "S3 can throw certain exceptions randomly. This function
  lets you automatically retry."
  [max-tries call & args]
  (if (> max-tries 1)
    (try
      (apply call args)
      (catch Exception e
        (Thread/sleep 100)
        (log/debugf "Exception trying [%s %s], retrying with %d tries to go"
                    call (string/join " " args) max-tries)
        (apply s3-retry (- max-tries 1) call args)))
    (apply call args)))

(defn s3-read-obj-bytes
  "Returns the contents of the given S3 object
  as a ByteBuffer, without subjecting them to an encoding
  like slurp does."
  [bucket key]
  (let [obj (s3-retry 5 s3/get-object :bucket-name bucket :key key)
        obj-size (get-in obj [:object-metadata :content-length])
        by (byte-array obj-size)
        stream (:object-content obj)]
    (loop [offset 0]
      (let [stored (.read stream by offset obj-size)]
        (when-not (< stored 0) ; -1 indicates EOF
          (recur (+ offset stored)))))
    (.close stream)
    (ByteBuffer/wrap by)))

(defn- s3-key-exists?
  [bucket key]
  (try
    (s3/get-object-metadata :bucket-name bucket
                            :key key)
    true
    (catch AmazonServiceException e
      (when-not (= 404 (.getStatusCode e)) ; returns nil if key nonexistent
        (throw e))))) ; not the error we're looking for

(defn- dataset-exists?
  "Returns true of if the given dataset exists in the given bucket,
  with the supplied key-prefix."
  [bucket key-prefix dataset]
  (s3-key-exists? bucket (dataset-marker-key key-prefix dataset)))

(defn- version-metadata
  "Gets metadata for a dataset version.
  full-key should have come from version-key."
  ([bucket full-key]
   (-> (s3/get-object :bucket-name bucket :key full-key)
       :object-content
       slurp
       (utils/parse-metadata true))))

(defn s3-list-objs-with-pagination
  "Lists all S3 objects in bucket, with prefix, handling pagination.
  Returnes a list containing S3ObjectSummarys."
  ([bucket prefix & [{:keys [start-at]}]]
   (let [request {:bucket-name bucket
                  :prefix prefix
                  :marker start-at}]
     (s3-list-objs-with-pagination request)))
  ([req]
   (let [listing (s3-retry 5 s3/list-objects req)
         objs (:object-summaries listing)]
     (if (:truncated? listing)
       (concat objs (lazy-seq (s3-list-objs-with-pagination
                                (assoc req :marker (:next-marker listing)))))
       objs))))

(defn s3-list-keys-with-pagination
  "Lists all S3 keys in bucket, with prefix, handling pagination."
  [bucket prefix & opts]
  (map :key (s3-list-objs-with-pagination bucket prefix opts)))

(defn- s3-oldest-key-with-prefix
  [bucket prefix]
  (let [objects-list (s3-list-objs-with-pagination bucket prefix)]
    (case (count objects-list)
      0 nil
      1 (:key (first objects-list))
      ; default: >1 listed object
      (->> objects-list
           (reduce (fn [v1 v2]
                     (if (.isBefore (:last-modified v1) (:last-modified v2))
                       v1 v2)))
           :key))))

; We must supply an input stream to put-object methods, but
; sometimes we store all information in the key and don't have
; any content to PUT.
(def empty-input-stream
  (ByteArrayInputStream. (byte-array 0)))

(defn- s3-create-empty-obj
  [bucket key]
  (s3-retry 5
            s3/put-object :bucket-name bucket
            :key key
            :input-stream empty-input-stream
            :metadata {:content-length 0}))

(defn- stale-temporary-objects
  "Used by write-index. This function filters the provided
  list of objects for stale temporaries and returns them.

  hash-keys-timestamps is a map that looks like {hash timestamp-written}.
  It's genereated from hash-listing, so it's just passed
  to avoid duplication of effort."
  [hash-listing hash-keys-timestamps]
  (for [obj hash-listing
        :let [key (-> obj :key last-segment)
              key-pieces (split-index-key-end key)
              ; 40-ch chunk hash like ab2319ffe
              hash (first key-pieces)
              ; nil or ffee222919
              old-hash (second key-pieces)
              ; nil or timestamp
              old-hash-ts (-> (get key-pieces 2)
                              str->timestamp)
              ; objects aren't modified after creation
              created-date (-> obj :last-modified DateTime.)]
        ; a hash is stale when
        ; it refers to an old-hash that still exists
        ; AND it's more than 120s old
        :when (and (not (= "nil" old-hash))
                   (= old-hash-ts (get hash-keys-timestamps old-hash))
                   (-> created-date
                       (.withDurationAdded 1000 120)
                       (.isBefore (DateTime.))))]
    (:key obj)))

(defn- new-hash-written-ok?
  "Used by write-index. Returns true if new-hash has been
  correctly written and it's OK to proceed with the final
  steps of write-index. Returns false if another writer is
  detected at the same coordinates and we must abort."
  [bucket prefix old-key new-key]
  (loop []
    (let [hash-listing (s3-list-keys-with-pagination bucket prefix)

          new-hash-present? (some #{new-key} hash-listing)

          unrecognized-hashes
          (->> hash-listing
               (remove #(= % new-key))
               (remove #(= % old-key)))]

      (if-not (empty? unrecognized-hashes)
        ; someone else is trying to write to the index: abort
        (do (log/tracef (str "abort: the following keys were not recogized"
                             "as belonging to this writer: %s")
                             (pr-str unrecognized-hashes))
            false)
        (if (not new-hash-present?)
          ; no one else has tried to write yet, but the object we
          ; wrote still hasn't appeared. retry.
          (do (log/tracef
                "replacing hash: waiting for key [%s] to appear" new-key)
              (recur))
          ; Our object has appeared. OK to proceed.
          true)))))

(deftype S3Index [bucket key-prefix dataset version-cache
                  client-opts var-name metadata]
  ;; table is a dataset-level table name
  proto/Index

  (target [_] {:metadata metadata :var-name var-name})

  ; Snarky comment: This protocol method was poorly designed.
  ; an S3Index receives a :version-id in the metadata;
  ; it should not allow you to request data from a different
  ; version.
  (chunk-at [_ coordinate version-id]
    (let [version-id (str version-id)]
    (log/tracef "requested chunk-at var [%s] coord %s for version [%s]"
                var-name (pr-str coordinate) version-id)
    (assertex (valid-version-id? version-id) "invalid version id")
    ; we need to find the most recent chunk at this coord, but it might
    ; be in a previous version from the one specified
    ;
    (loop [versions (->> version-cache
                         (map :version) ; get version keys
                         ; omit versions newer than what we want
                         (drop-while (partial not= version-id)))]
      (log/tracef "versions selected %s" (pr-str versions))
      (if (= 0 (count versions))
        ; no chunks found at this coord for any version <= version-id
        (do (log/tracef "no chunk found for var [%s] coord %s for version [%s]"
                        var-name (pr-str coordinate) version-id)
            nil)
        (if-let
          [candidate
           (->> (index-dir key-prefix dataset var-name coordinate (first versions))
                (s3-oldest-key-with-prefix bucket)
                last-segment)]
          (let [chunk-hash (first (split-index-key-end candidate))]
            (log/tracef (str "located chunk hash [%s] (chunk entry [%s]) "
                             "for var [%s] coord %s version [%s]")
                        chunk-hash candidate var-name (pr-str coordinate) version-id)
            chunk-hash)
          (recur (rest versions)))))))

  (chunk-at [_ coordinate]
    (.chunk-at _ coordinate (:version-id metadata)))

  ; Updating the index is a several-step process designed to
  ; handle conflicts with multiple writers, given S3's limited
  ; consistency guarantees. It uses optimistic concurrency
  ; control (but with extensive checking).
  (write-index [_ coordinate old-hash new-hash]
    (log/debugf (str "var [%s] at coordinate %s, version %s "
                     "attempting to replace hash [%s] with [%s]")
                var-name (pr-str coordinate) (:version-id metadata) old-hash new-hash)
    (assertex (not (nil? new-hash)) "s3 backend does not support nil new-hash")

    (let [prefix (index-dir key-prefix dataset var-name
                            coordinate (:version-id metadata))
          ; Step 1: LIST all hashes at coord for this version.
          hash-listing (s3-retry
                         5 s3-list-objs-with-pagination
                         bucket prefix)
          hashes (map (comp last-segment :key)
                      hash-listing)
          hash-keys-timestamps (into {} (for [obj hash-listing
                                              :let [key (-> obj :key last-segment)
                                                    hash (-> key split-index-key-end
                                                             first)
                                                    timestamp (:last-modified obj)]]
                                          [hash timestamp]))

          ; Step 2: Identify and DELETE any stale temporary hashes.
          stale-temps (stale-temporary-objects hash-listing hash-keys-timestamps)]
      (log/tracef "deleting temporary hashes %s" (pr-str stale-temps))
      (map #(s3/delete-object bucket %) stale-temps)

      ; Verify that we can proceed with Step 3.
      ; We can go if no one else is trying to modify this part of the index.
      (let [active-keys (difference (set hashes)
                                    (set (map last-segment stale-temps)))]
        (if-not (or
                  ; no chunk previously written for this version
                  (and (= nil old-hash) (= 0 (count active-keys)))
                  ; replacing an existing chunk hash
                  (and (= 1 (count active-keys))
                       (= old-hash (-> (first active-keys)
                                       split-index-key-end
                                       first)))
                  ; old-hash refers to the hash at a previous version; we don't
                  ; delete it, but it's ok to proceed
                  (and (= 0 (count active-keys))
                       (= old-hash (.chunk-at _ coordinate))))
          (do (log/debugf (str "Could not update index for [%s] at coordinate [%s] "
                               "(attempting to replace hash [%s] with [%s]). "
                               "Unexpected entries in index. "
                               "Either old-hash incorrect or someone else is "
                               "modifying the index")
                          var-name (pr-str coordinate) old-hash new-hash)
              false)
          (if (= old-hash new-hash) ; no-op if they're the same (still return true)
            true
            (let
              ; the DELETE of old-key behaves correctly
              ; even when (first active-keys) is nil
              ; (deleting a nonexisting key is a no-op)
              [old-key (str prefix "/" (first active-keys))
               old-timestamp (get hash-keys-timestamps old-hash)
               old-timestamp (if old-timestamp
                               (.getMillis old-timestamp)
                               nil)
               new-key (str prefix "/" (index-key-end
                                         new-hash old-hash
                                         old-timestamp))
               create-time (DateTime.)]

              ; Step 3: PUT a new object whose key is new-key.
              (s3-create-empty-obj bucket new-key)
              (log/tracef "created temporary new-key object: %s" new-key)

              ; Step 4: LIST the prefix again and make sure that
              ; we're still the only writer in town.
              ; Step 5: if the Step 4 check passes, verify that there
              ; are at least 30 s left on the clock before the new-hash
              ; object expires. This is a safety measure. (An temporary
              ; object has 120 s before it becomes stale).
              (if (and (new-hash-written-ok? bucket prefix old-key new-key)
                       (-> create-time
                           (.withDurationAdded 1000 90)
                           (.isAfter (DateTime.))))
                ; Proceed to Step 6: delete old-key.
                (do (s3-retry
                      5 s3/delete-object :bucket-name bucket :key old-key)
                    (log/tracef "var [%s] at coordinate %s: replaced hash [%s] with [%s]"
                                var-name (pr-str coordinate) old-hash new-hash)
                    true)
                ; Something went wrong: abort
                (do
                  (log/debugf (str "could not update index for [%s] at coordinate [%s] "
                                   "(attempting to replace hash [%s] with [%s]): "
                                   "the operation took too long or "
                                   "someone else is modifying the index")
                              var-name (pr-str coordinate) old-hash new-hash)
                  (s3-retry 5 s3/delete-object :bucket-name bucket :key new-key)
                  false))))))))

  (flush-index [_]
     (log/debugf "S3Index flush method called: no-op")))

; Chunk store (retrieves chunk blobs based on hash key)
; We don't need to worry about multiple writers here, because
; if they each write to the same hash then they will all write
; exactly the same chunk blob.
(deftype S3ChunkStore [bucket key-prefix dataset client-opts]
  proto/ChunkStore

  (read-chunk [_ hash]
    (log/debugf "read-chunk %s" hash)
    (when (or (empty? hash) (not (string? hash)))
      (throw
        (IllegalArgumentException. "hash must be a non-empty string")))
    (if-let [item (s3-read-obj-bytes
                    bucket (chunk-key key-prefix dataset hash))]
      (do (log/tracef "located chunk with hash %s" hash) item)
      (do (log/tracef "not found: chunk with hash %s" hash)
          (throw (IllegalArgumentException.
                   (format "No chunk was found for hash %s" hash))))))

  ;; fake chunk-refs until we come up with something better
  (chunk-refs [_ hash]
    (log/debugf "chunk-refs for hash [%s] not implemented for S3 backend" hash)
    1)

  (write-chunk [_ hash ref-count content-bytes]
    (log/debugf "attempting to write chunk for hash [%s]" hash)
    (when (or (empty? hash) (not (string? hash)))
      (throw
        (IllegalArgumentException. "hash must be a non-empty string")))
    (when-not (integer? ref-count)
      (throw
        (IllegalArgumentException. "ref-count must be an integer")))
    (when-not (instance? ByteBuffer content-bytes)
      (throw
        (IllegalArgumentException. "bytes must be a ByteBuffer instance")))
    (when-not (pos? (.remaining content-bytes))
      (throw
        (IllegalArgumentException. "Chunk has no remaining bytes")))

    (let [by (bytes (byte-array (.remaining content-bytes)))]
      (.get content-bytes by)
      (s3-retry 5
                s3/put-object :bucket-name bucket
                :key (chunk-key key-prefix dataset hash)
                :input-stream (ByteArrayInputStream. by)
                :metadata {:content-length (alength by)}))
    (.rewind content-bytes)
    ; logged only when an exception isn't thrown above
    (log/tracef "wrote chunk for hash [%s]" hash)
    nil)

  ;; fake chunk-refs until we come up with something better
  (update-chunk-refs [_ hash delta]
    (log/tracef "update-chunk-refs by %d for %s (non-operational)"
                delta hash)
    nil))

(deftype S3Connection [bucket key-prefix dataset client-opts]
  proto/Connection

  (index [this var-name {:keys [version-id] :as metadata} options]
    (log/infof "connecting to index at [%s] for dataset [%s] var [%s] version [%s]"
               (str bucket "/" key-prefix) dataset var-name version-id)
    (assertex (valid-version-id? version-id) "invalid version id")

    ; The version cache is an annoying invention to solve a performance problem.
    ; Because the backend IndexStore protocol requires .chunk-at to return the most
    ; recent (<= specified version-id) hash in the index, it needs to know
    ; the chronological order of the versions, which is provided by .versions.
    ; However this creates a problem when calling .chunk-at with an uncommitted
    ; version-id, because it won't be listed in .versions. Each S3Index is therefore
    ; supplied with a version cache upon creation. It's annoying because an S3Index
    ; is created with a fixed view of the available versions and cannot update itself
    ; if a writer commits a new version.

    (let [version-cache (vec (.versions this nil))
          version-cache (let [id-loc (.indexOf (map :version version-cache)
                                               (str version-id))]
                          (if (not= -1 id-loc) ; -1 = not found
                            ; if version-id has been committed, that's fine
                            ; this reader will use all versions up to that point
                            (subvec version-cache id-loc)
                            ; otherwise use all versions, and place this uncommitted
                            ; version at the head of the version cache
                            (cons {:timestamp (DateTime.)
                                   :version (str version-id)
                                   :metadata metadata} version-cache)))]
      (log/tracef "for version %s, using version-cache %s"
                  (str version-id) (pr-str version-cache))
      (->S3Index bucket key-prefix dataset version-cache
                 client-opts var-name metadata)))

  (write-version [_ metadata]
    ;; TODO: commit must fail if last version != parent-version
    ; WARNING: this is not safe for multiple writers.
    ; No locking is employed, and unlike the write-index method
    ; no attempts are made to handle multiple writer cases correctly.

    (let [{:keys [version-id]} metadata]
      (log/debugf "writing version [%s] of dataset [%s]" version-id dataset)
      (assertex (valid-version-id? version-id) "invalid version id")
      (let [previous-version-key
            (first (s3-list-keys-with-pagination
                     bucket (version-registry-dir key-prefix dataset)))
            ; first item listed is the most recent version
            previous-version-hex-code
            (if previous-version-key
              (hex-code-from-version-key previous-version-key)
              (+ 1 0x7FFFFFFF)) ; the first version hex code is 2^31 - 1
            metadata-string (utils/generate-metadata metadata)
            metadata-bytes (.getBytes metadata-string)]

        ; critical section: if another writer modifies the index here,
        ; there will be an inconsistency
        (s3-retry 5
                  s3/put-object
                  :bucket-name bucket
                  :key (version-key key-prefix dataset
                                    (- previous-version-hex-code 1) version-id)
                  :input-stream (ByteArrayInputStream. metadata-bytes)
                  :metadata {:content-length (alength metadata-bytes)})))
    nil)

  (chunk-store [_ options]
    (->S3ChunkStore bucket key-prefix dataset client-opts))

  ;; TODO make this useful
  (get-stats [_]
    {})

  (metadata [_ version]
    (->>
      ; figure out the key
      (version-registry-dir key-prefix dataset)
      (s3-list-keys-with-pagination bucket)
      (filter #(.contains %1 version))
      first
      ; get the metadata
      (version-metadata bucket)))

  (versions [_ opts]
    (let [version-objs
          (s3-list-objs-with-pagination bucket (version-registry-dir
                                                 key-prefix dataset))]
      (map (fn [version-obj]
             {:timestamp (DateTime. (:last-modified version-obj))
              :version (last-segment (:key version-obj))
              :metadata (version-metadata bucket (:key version-obj))})
             version-objs))))

(deftype S3Schema [bucket key-prefix client-opts]
  proto/Schema

  (create-dataset [_ name]
    (assertex (and (string? name) (not (string/blank? name)))
              "dataset name must be a non-empty string")
    (assertex (not (dataset-exists? bucket key-prefix name))
              "dataset already exists")
    (when (dataset-exists? bucket key-prefix name)
      (throw (Exception. "you suck")))
    (s3-create-empty-obj bucket (dataset-marker-key key-prefix name))
    nil)

  ; This does not actually delete the data, because S3 takes so long
  ; to reply to requests that deleting could take hours if the dataset
  ; is large. However, once the dataset marker is deleted, a asynchronous
  ; cleanup job can destroy the data.
  (destroy-dataset [_ name]
    (when (dataset-exists? bucket key-prefix name)
      (s3/delete-object bucket (dataset-marker-key key-prefix name))))

  (list-datasets [_]
    (let [a (->> (s3-list-keys-with-pagination bucket (dataset-registry-dir key-prefix))
         (map last-segment))]
      a))

  (connect [_ dataset-name]
    (assertex (dataset-exists? bucket key-prefix dataset-name)
              "dataset nonexistent")
    (->S3Connection bucket key-prefix dataset-name client-opts)))


;;----------------------------------------
;; Helper functions for making the schema.

(defn- random-session-name []
  (.substring (str (UUID/randomUUID)) 0 32))

(defn store-spec->client-opts
  "Creates a map with an AWS provider (:provider).

  AWS has a default provider which looks for credentials in this order:
    Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
    Environment Variables - AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY
    Java System Properties - aws.accessKeyId and aws.secretKey
    Credential profiles file at the default location (~/.aws/credentials)
    Instance profile credentials through the EC2 metadata service

  If a role-arn is given that role will be assumed either with the given
  provider or by using the default provider.

  If a provider is given but no role-arn, the provider will be used.

  If neither a provider nor role-arn are given, the default provider
  will be used.

  config {
    :endpoint - (optional)
    :provider - (optional)
    :role-arn - (optional)
    :session-name - (optional)
  }"
  [& [{:keys [provider role-arn session-name] :as store-spec}]]
  (let [session-name (or session-name (random-session-name))
        provider (cond
                   (and provider role-arn) (STSAssumeRoleSessionCredentialsProvider.
                                             provider
                                             role-arn
                                             session-name)
                   provider provider
                   role-arn (STSAssumeRoleSessionCredentialsProvider.
                              role-arn
                              session-name)
                   :else (DefaultAWSCredentialsProviderChain.))]
    (merge store-spec {:provider provider})))

(defn mk-schema [store-spec]
  "Makes a S3Schema from the store-spec

  A store spec is a map with the following entries

   :root       - the root of the store (format: bucket/key-prefix)
   :db-version - the version of this library
   :session-id - (optional) a unique identifier to id this session
   :role-arn   - (optional) the role to use dynamodb as

   returns a DynamoDBSchema
   "
  (let [[bucket key-prefix] (string/split (:root store-spec) #"/" 2)]
    (->S3Schema bucket key-prefix (store-spec->client-opts store-spec))))
