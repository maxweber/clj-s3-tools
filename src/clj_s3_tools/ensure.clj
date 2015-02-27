(ns clj-s3-tools.ensure
  (:require [clj-s3-tools.list :as l]
            [clj-s3-tools.utils :refer [smap]]))

(defn ensure-ends-with-slash [path name]
  (when (and (> (count path) 0)
             (not (.endsWith path "/")))
    (throw (ex-info (str path " has to end with a slash if it is not empty")
                    {(keyword name) path}))))

(defn ensure-folder-is-empty [bucket-name folder-path]
  (when (not (empty? (l/list-object-summaries bucket-name folder-path)))
    (throw (ex-info "folder is not empty"
                    (smap bucket-name folder-path)))))
