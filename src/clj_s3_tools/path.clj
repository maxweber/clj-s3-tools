(ns clj-s3-tools.path
  (:require [clojure.string :as str]))

(defn last-dir [path]
  (last (str/split path #"/")))
