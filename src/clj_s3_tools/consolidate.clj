(ns clj-s3-tools.consolidate
  (:require [clojure.java.io :as io]
            [amazonica.aws.s3 :as s3]
            [clj-s3-tools.utils :refer [io-map]]
            [clj-s3-tools.ensure :as e]))

(defn merge-files
  [dest-bucket dest-path object-summaries]
  (let [tmp-file (java.io.File/createTempFile "merge-files" "")]
    (with-open [out (io/output-stream tmp-file)]
      (doseq [{:keys [key bucket-name]} object-summaries]
        (with-open [in (:input-stream (s3/get-object bucket-name key))]
          (io/copy
           in
           out))))
    (s3/put-object
     dest-bucket
     dest-path
     tmp-file)
    (.delete tmp-file)))

(defn group-objects [target-size object-summaries]
  ;; TODO: group-objects only handles a bunch of small files (in
  ;;       compairison to the target-size) appropriately. Also it
  ;;       should adhere to S3 file size limits.
  (let [result (reduce
                (fn [{:keys [current-size current-group] :as state} object-summary]
                  (if (< current-size target-size)
                    (assoc
                     state
                     :current-group (conj current-group object-summary)
                     :current-size (+ current-size (:size object-summary)))
                    (assoc
                     state
                     :current-group [object-summary]
                     :current-size (:size object-summary)
                     :groups (conj (:groups state) current-group))))
                {:current-size 0
                 :current-group []
                 :groups []}
                object-summaries)]
    (conj (:groups result) (:current-group result))))

(def default-consolidated-file-size (* 1024 1024 128))

(def default-dest-file-name-fn (fn [part-number] (str part-number)))

(defn plan-consolidate-files
  [{:keys [dest-bucket dest-path object-summaries consolidated-file-size
           dest-file-name-fn]
    :or {consolidated-file-size default-consolidated-file-size
         dest-file-name-fn default-dest-file-name-fn}}]
  (e/ensure-ends-with-slash dest-path "dest-path")
  (let [groups (group-objects consolidated-file-size object-summaries)]
    {:actions
     (map-indexed
      (fn [idx group]
        {:group group
         :dest-bucket dest-bucket
         :dest-key (str dest-path (dest-file-name-fn idx))})
      groups)}))

(defn extract-folder-path [key]
  (second (re-find #"(.*/)" key)))

(defn- destinct-destinations [consolidate-files-plan]
  (distinct
   (map
    (juxt :dest-bucket
          (comp extract-folder-path :dest-key))
    (:actions consolidate-files-plan))))

(defn consolidate-files
  [consolidate-files-plan & [{:keys [parallelism] :or {parallelism 10}}]]
  (doseq [[dest-bucket dest-path] (destinct-destinations consolidate-files-plan)]
    (e/ensure-folder-is-empty dest-bucket dest-path))
  (let [actions (:actions consolidate-files-plan)]
    (io-map
     parallelism
     (fn [{:keys [dest-bucket dest-key group]}]
       (merge-files
        dest-bucket
        dest-key
        group))
     actions)
    true))

(comment
  ;; example
  (let [plan (plan-consolidate-files
              {:dest-bucket "test"
               :dest-path "consolidated/"
               :object-summaries
               (clj-s3-tools.list/list-object-summaries
                "test"
                "results/")})]
    (consolidate-files plan)))
