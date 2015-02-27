(ns clj-s3-tools.list
  (:require [amazonica.aws.s3 :as s3]))

(defn list-object-summaries [bucket-name prefix & [marker]]
  (let [result (s3/list-objects
                {:bucket-name bucket-name
                 :prefix prefix
                 :marker marker})]
    (concat
     (:object-summaries result)
     (when-let [next-marker (:next-marker result)]
       (lazy-seq (list-object-summaries
                  bucket-name
                  prefix
                  next-marker))))))
