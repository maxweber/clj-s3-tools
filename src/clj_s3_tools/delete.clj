(ns clj-s3-tools.delete)

(defn delete-objects
  "Deletes all keys (S3 objects) in the given bucket. Returns a list
  of deleted keys."
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
