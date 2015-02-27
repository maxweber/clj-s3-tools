(ns clj-s3-tools.list
  (:require [amazonica.aws.s3 :as s3]
            [clj-s3-tools.ensure :as e]
            [clj-s3-tools.path :as p]))

(defn- list-all [opts]
  (let [result (s3/list-objects opts)]
    (cons
     result
     (when-let [next-marker (:next-marker result)]
       (lazy-seq (list-all
                  (assoc opts :marker next-marker)))))))

(defn list-object-summaries
  ([bucket-name prefix]
   (mapcat
    :object-summaries
    (list-all
     {:bucket-name bucket-name
      :prefix prefix}))))

(defn list-sub-dirs
  "Lists all subdirectories of 'dir-path/' in the given S3 bucket."
  [bucket-name dir-path]
  (e/ensure-ends-with-slash dir-path "dir-path")
  (map
   p/last-dir
   (distinct
    (mapcat
     :common-prefixes
     (list-all
      {:bucket-name bucket-name
       :prefix dir-path
       :delimiter "/"})))))
