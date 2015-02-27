(ns clj-s3-tools.list
  (:require [amazonica.aws.s3 :as s3]))

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
