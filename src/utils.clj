(ns utils
  (:require
   [charred.api :refer [parse-json-fn]]))

(def read-json
  (parse-json-fn {:key-fn keyword
                  :async? false
                  :bufSize 1024}))
