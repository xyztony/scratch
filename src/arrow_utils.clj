(ns arrow-utils
  (:require
   [tech.v3.libs.arrow :as arrow]
   [tech.v3.dataset :as ds]
   [tech.v3.resource :as resource]
   [clojure.java.io :as io]
   [clojure.string :as str]))

(def ^:private default-options
  ;; Use mmap by default https://techascent.github.io/tech.ml.dataset/tech.v3.libs.arrow.html
  {:open-type :mmap
   :integer-datetime-types? true ; Use efficient integer representation for dates
   :text-as-strings? true ; Convert text to strings for easier handling
   })

(defn file-extension
  "Get the file extension to determine if it's an Arrow file"
  [path]
  (when-let [filename (if (string? path)
                        path
                        (some->> path io/file .getName))]
    (some->> (re-find #"\.([^.]+)$" filename)
             last
             str/lower-case)))

(defn arrow-file? [path] (#{"arrow" "arrows"} (file-extension path)))

(defn open-arrow-dataset
  ([path]
   (open-arrow-dataset path {}))
  ([path options]
   (let [opts (merge default-options options)]
     (arrow/stream->dataset path opts))))

(def is-file? (fn [v] (.isFile v)))

(def ticker-yyyy-mm-dd__hh_mm_ss-patern-str
  "%s-(\\d{4}-\\d{2}-\\d{2}__\\d{2}_\\d{2}_\\d{2})\\.%s$")

(defn ticker-yyyy-mm-dd__hh_mm_ss-pattern
  [ticker ext]
  (-> ticker-yyyy-mm-dd__hh_mm_ss-patern-str
      (format ticker ext)
      re-pattern))

(defn- file->pattern-matches [pattern file]
  (when-let [matches
             (and (is-file? file)
                  (re-find pattern (str file)))]
    {:file file
     :matches matches}))

(comment
  (let [pattern
        (ticker-yyyy-mm-dd__hh_mm_ss-pattern "btc" "arrow")

        files-with-matches
        (->> (file-seq (io/file "."))
             (keep (partial file->pattern-matches pattern)))]
    (resource/releasing!
     ;; :todo
     ))

  (require '[tech.v3.dataset.reductions :as ds-reduce])
  (require '[tech.v3.dataset.rolling :refer [rolling] :as ds-roll])
  (let [btc (arrow/stream->dataset
             "./btc-2025-03-09__03_06_54.arrow"
             {:key-fn keyword})]
    #_(ds-reduce/aggregate
       {:n-elems (ds-reduce/row-count)
        :price-avg (ds-reduce/mean :px)
        :price-sum (ds-reduce/sum :px)
        :price-med (ds-reduce/prob-median :px)
        :price-iqr (ds-reduce/prob-interquartile-range :px)
        :n-dates (ds-reduce/count-distinct :tid :int32)}
       btc)
    (-> (ds/sort-by-column btc :time <)
        (rolling {:window-type :fixed
                  ;; :column-name :time
                  :window-size 5
                  :relative-window-position :left}
                 {:max (ds-roll/max :px)})
        (ds/head 50)
        clojure.pprint/pprint))
  
  ,)
