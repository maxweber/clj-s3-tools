(ns clj-s3-tools.delete
  (:require [amazonica.aws.s3 :as s3]
            [clj-s3-tools.list :as l]))

(defn delete-objects
  "Deletes all keys (S3 objects) in the given bucket. Returns a
  sequence of deleted keys."
  [bucket-name keys]
  (doall
   (mapcat
    (fn [batch]
      (map
       :key
       (:deleted-objects
        (s3/delete-objects
         {:keys (vec batch)
          :bucket-name bucket-name}))))
    (partition-all 1000 keys))))

(defn delete-dir
  "Deletes all keys under the prefix dir-path in the given
  bucket. Returns a sequence of deleted keys."
  [bucket-name dir-path]
  {:pre [(.endsWith dir-path "/")]}
  (let [keys (map :key (l/list-object-summaries bucket-name dir-path))]
    (delete-objects bucket-name keys)))
