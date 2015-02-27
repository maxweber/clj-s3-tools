(ns clj-s3-tools.size
  (:require [clj-s3-tools.list :as l]))

(defn folder-size [bucket-name prefix]
  (reduce + (map :size (l/list-object-summaries bucket-name prefix))))

(defn in-mb [byte-count]
  (/ byte-count 1024 1024.0))

(defn in-gb [byte-count]
  (/ byte-count 1024 1024 1024.0))
