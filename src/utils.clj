(ns utils
  (:require
   [clojure.core.async :as a]
   [clojure.pprint :as pp]
   [charred.api :refer [parse-json-fn]]))

(defn next-or-empty [coll]
  (or (and (seq coll) (next coll)) []))

(def read-json
  (parse-json-fn {:key-fn keyword
                  :async? false
                  :bufSize 1024}))

(defn monitoring [{:keys [report-chan error-chan]}]
  (prn "========= monitoring start")
  (a/thread
    (loop []
      (let [[val port] (a/alts!! [report-chan error-chan])]
        (if (nil? val)
          (prn "========= monitoring shutdown")
          (do
            (prn (str "======== message from " (if (= port error-chan) :error-chan :report-chan)))
            (pp/pprint val)
            (recur))))))
  nil)
