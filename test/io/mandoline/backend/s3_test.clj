(ns io.mandoline.backend.s3-test
  (:require
    [amazonica.aws.s3 :as aws-s3]
    [clojure.test :refer :all]
    [clojure.tools.logging :as log]
    [clojure.string :refer [split join blank?]]
    [environ.core :refer [env]]
    [io.mandoline :as db]
    [io.mandoline.impl :as impl]
    [io.mandoline.impl.protocol :as proto]
    [io.mandoline.slice :as slice]
    [io.mandoline.backend.s3 :as s3back]
    [io.mandoline.test.utils :refer [random-name
                                             with-and-without-caches
                                             with-temp-db]]
    [io.mandoline.test
     [concurrency :refer [lots-of-overlaps
                          lots-of-processes
                          lots-of-tiny-slices]]
     [entire-flow :refer [entire-flow]]
     [failed-ingest :refer [failed-write]]
     [grow :refer [grow-dataset]]
     [invalid-metadata :as invalid-metadata]
     [linear-versions :refer [linear-versions]]
     [nan :refer [fill-double
                  fill-float
                  fill-short]]
     [overwrite :refer [overwrite-dataset
                        overwrite-extend-dataset]]
     [scalar :refer [write-scalar]]
     [shrink :refer [shrink-dataset]]]
    [io.mandoline.test.protocol
     [chunk-store :as chunk-store]
     [schema :as schema]])

  (:import
    [java.nio ByteBuffer]
    [org.apache.commons.codec.digest DigestUtils]
    [com.amazonaws AmazonClientException]
    [com.amazonaws.auth BasicAWSCredentials]
    [io.mandoline.slab Slab]))

(def ^:private test-dataset-name "test-dataset")

(defn- s3-delete-bulk
  "Deletes all S3 keys beginning with prefix, in bucket."
  [bucket prefix]
  (let [listing (s3back/s3-list-keys-with-pagination bucket prefix)]
    (doseq [piece (partition-all 1000 listing)]
      (aws-s3/delete-objects :bucket-name bucket :keys (vec piece)))))

(defn setup
  "Create a random store spec for testing the S3 Doc Brown
  backend.

  This function is intended to be used with the matching teardown
  function."
  []
  (assert (not (blank? (env :mandoline-s3-test-path)))
          (str "S3 backend integration tests require a MANDOLINE_S3_TEST_PATH "
               "environment variable: bucket/test-prefix"))
  (let [root (join "/" [(env :mandoline-s3-test-path) (random-name)])
        dataset test-dataset-name]
    {:store "io.mandoline.backend.s3/mk-schema"
     :root root
     :dataset dataset}))

(defn teardown
  "Tears down after tests (deletes the S3 objects stored by the backend)."
  [store-spec]
  (let [[bucket prefix] (split (:root store-spec) #"/" 2)]
    (s3-delete-bulk bucket prefix))
  nil)

(deftest ^:integration test-s3-chunk-store-properties
  (let [store-specs (atom {}) ; map of ChunkStore instance -> store-spec
        setup-chunk-store (fn []
                            (let [store-spec (setup)
                                  dataset (:dataset store-spec)
                                  c (-> (doto (s3back/mk-schema store-spec)
                                          (.create-dataset dataset))
                                        (.connect dataset)
                                        (.chunk-store {}))]
                              (swap! store-specs assoc c store-spec)
                              c))
        teardown-chunk-store (fn [c]
                               (let [store-spec (@store-specs c)]
                                 (teardown store-spec)))
        num-chunks 10]

    (let [chunk-store (setup-chunk-store)
          r (java.util.Random.)]
      (try
        (testing
          (format "Single-threaded tests of ChunkStore %s:" (type chunk-store))
          (is (satisfies? proto/ChunkStore chunk-store)
              (format
                "%s instance implements the ChunkStore protocol"
                (type chunk-store)))
          (let [chunks-to-write (chunk-store/generate-chunks-to-write r num-chunks)
                written-chunks (map first chunks-to-write)
                nonexistent-chunks (chunk-store/rand-hash r num-chunks)
                chunk-refs-to-update (->> written-chunks
                                          (map (fn [hash]
                                                 [hash (chunk-store/rand-range r -100 100)]))
                                          (shuffle))]
            (chunk-store/test-write-chunk-single-threaded
              chunk-store chunks-to-write :skip-refs? true)
            (chunk-store/test-read-chunk-single-threaded
              chunk-store written-chunks nonexistent-chunks)))
        ; The S3 backend does not support chunk ref counting
        ; (because it would pose a concurrency problem)
        ; so we don't run those tests (which are in other backends).
        (finally (teardown-chunk-store chunk-store))))))

(deftest ^:integration test-dataset-mgmt
  (let [store-spec (setup)
        schema (s3back/mk-schema store-spec)
        dataset1 "ds1"
        dataset2 "ds2"]
    (try
      (is (empty? (.list-datasets schema)) "backend begins life devoid of datasets")
      (.create-dataset schema dataset1)

      (let [l (.list-datasets schema)]
        (is (= 1 (count l)))
        (is (= dataset1 (first l)) "backend knows of a created dataset"))

      (.destroy-dataset schema "ds2") ; should be a noop

      (let [l (.list-datasets schema)]
        (is (= 1 (count l)))
        (is (= dataset1 (first l)) "nothing changed when a nonexistent dataset is deleted"))

      (.destroy-dataset schema "ds1")
      (is (empty? (.list-datasets schema)) "backend properly deletes datasets")

      (finally (teardown store-spec)))))

(deftest ^:integration test-index-mgmt
  (let [store-spec (setup)
        schema (s3back/mk-schema store-spec)
        dataset1 "california"
        var1 "ddr"
        version-id "01031996"
        _ (.create-dataset schema dataset1)
        conn (.connect schema dataset1)
        index (.index conn var1 {:version-id version-id} nil)
        coords [1 1]]
    (try
      (is (.write-index index coords nil "ffe"))
      (is (= "ffe" (.chunk-at index coords))
          "write-index correctly writes a new coord")

      (is (not (.write-index index coords nil "ff7"))
               "write-index fails with incorrect old hash")

      (is (= "ffe" (.chunk-at index coords))
          "write-index doesn't change chunk when called with bad args")

      (is (.write-index index coords "ffe" "ff8"))
      (is (= "ff8" (.chunk-at index coords))
          "write-index correctly writes a new hash to an existing coord")

      (finally (teardown store-spec)))))

(deftest ^:integration test-versions-mgmt
  (let [store-spec (setup)
        schema (s3back/mk-schema store-spec)
        dataset1 "massachusetts"
        var1 "ddr"
        version-id1 "fbf3c"
        version-id2 "fbf4e"
        _ (.create-dataset schema dataset1)
        conn (.connect schema dataset1)
        index1 (.index conn var1 {:version-id version-id1} nil)
        coords [1 1]
        coords2 [1 2]]
    (try
      (is (.write-index index1 coords nil "abc"))
      (is (= "abc" (.chunk-at index1 coords))
          "write-index correctly writes a new coord")

      (is (.write-index index1 coords "abc" "abcd"))
      (is (= "abcd" (.chunk-at index1 coords))
          "write-index correctly writes a new hash to an existing coord")


      (.write-version conn {:version-id version-id1})
      (let [v (.versions conn nil)]
        (is (= 1 (count v)))
        (is (= version-id1 (:version (first v)))))

      ; connect to a second version
      (let [index2 (.index conn var1 {:version-id version-id2} nil)]
        ; chunk written in a previous version
        (is (= "abcd" (.chunk-at index2 coords)))

        (is (.write-index index2 coords "abcd" "abcde"))
        (is (= "abcde" (.chunk-at index2 coords)))

        (is (.write-index index2 coords "abcde" "abcdef"))
        (is (= "abcdef" (.chunk-at index2 coords)))

        (is (= "abcd" (.chunk-at index2 coords version-id1)))

        ; chunk not written in a previous version
        (is (.write-index index2 coords2 nil "eee"))
        (is (= "eee" (.chunk-at index2 coords2))))

      (.write-version conn {:version-id version-id2})

      (let [v (.versions conn nil)]
        (is (= 2 (count v)))
        (is (= version-id2 (:version (first v)))))

      (let [index3 (.index conn var1 {:version-id version-id1} nil)]
        (is (= "abcd" (.chunk-at index3 coords)))
        (is (= nil (.chunk-at index3 coords2))
            "reader for an earlier version should not see a chunk written later"))

      (finally (teardown store-spec)))))

(deftest ^:integration s3-entire-flow
  (with-and-without-caches
    (entire-flow setup teardown)))

(deftest ^:integration s3-grow-dataset
  (with-and-without-caches
    (grow-dataset setup teardown)))

(deftest ^:integration s3-shrink-dataset
  (with-and-without-caches
    (shrink-dataset setup teardown)))

(deftest ^:integration s3-overwrite-dataset
  (with-and-without-caches
    (overwrite-dataset setup teardown)))

(deftest ^:integration s3-overwrite-extend-dataset
  (with-and-without-caches
    (overwrite-extend-dataset setup teardown)))

(deftest ^:integration s3-invalid-metadata
  (with-and-without-caches
    (invalid-metadata/invalid-metadata setup teardown)))

(deftest ^:integration s3-change-metadata
  (with-and-without-caches
    (invalid-metadata/change-metadata setup teardown)))

(deftest ^:integration s3-lots-of-tiny-slices
  (with-and-without-caches
    (lots-of-tiny-slices setup teardown)))

(deftest ^:integration s3-lots-of-processes-ordered
  (with-and-without-caches
    (lots-of-processes setup teardown false)))

(deftest ^:integration s3-lots-of-processes-unordered
  (with-and-without-caches
    (lots-of-processes setup teardown true)))

(deftest ^:integration s3-lots-of-overlaps
  (with-and-without-caches
    (lots-of-overlaps setup teardown)))

(deftest ^:integration s3-write-scalar
  (with-and-without-caches
    (write-scalar setup teardown)))

(deftest ^:integration s3-write-fail-write
  (with-and-without-caches
    (failed-write setup teardown)))

(deftest ^:integration s3-linear-versions
  (with-and-without-caches
    (linear-versions setup teardown)))

