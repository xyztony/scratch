(ns arrow-utils
  (:require
   [tech.v3.libs.arrow :as arrow]
   [tech.v3.dataset :as ds]
   [tech.v3.resource :as resource]
   [tick.core :as t]
   [ham-fisted.lazy-noncaching :as hfnc]
   [medley.core :as medley]
   [clojure.java.io :as io]
   [clojure.string :as str]))

(def ^:private default-options
  ;; Use mmap by default https://techascent.github.io/tech.ml.dataset/tech.v3.libs.arrow.html
  {:open-type :mmap
   :integer-datetime-types? true ; Use efficient integer representation for dates
   :text-as-strings? true ; Convert text to strings for easier handling
   :key-fn keyword})

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

(defn is-file? [v] (.isFile v))

(def ticker-yyyy-mm-dd__hh_mm_ss-patern-str
  "%s-(\\d{4}-\\d{2}-\\d{2}__\\d{2}_\\d{2}_\\d{2})\\.%s$")

(defn ticker-yyyy-mm-dd__hh_mm_ss-pattern
  [ticker ext]
  (-> ticker-yyyy-mm-dd__hh_mm_ss-patern-str
      (format ticker ext)
      re-pattern))

(defn ticker-yyyy-mm-dd__hh_mm_ss->date
  [date-str]
  (t/parse-date-time date-str (t/formatter "yyyy-MM-dd__HH_mm_ss")))



(defn- file->pattern-matches [pattern file]
  (when-let [matches
             (and (is-file? file)
                  (re-find pattern (str file)))]
    {:file file
     :matches matches}))

(def date-match-comp
  (comparator
   (fn [a b]
     (t/<
      (ticker-yyyy-mm-dd__hh_mm_ss->date (last a))
      (ticker-yyyy-mm-dd__hh_mm_ss->date (last b))))))

(defn concat-arrow-files
  [{:keys [process-fn arrow] :as options} paths]
  (let [opts (merge default-options arrow)]
    (->> paths
         (mapcat #(arrow/stream->dataset-seq % opts))
         (hfnc/map process-fn)
         (apply ds/concat))))

(comment
  (let [pattern (ticker-yyyy-mm-dd__hh_mm_ss-pattern "btc" "arrow")

        files-with-matches
        (->> (file-seq (io/file "."))
             (keep (partial file->pattern-matches pattern)))

        process-fn (fn [ds] (ds/sort-by-column ds :time))]
    (->> files-with-matches
         (sort-by :matches date-match-comp)
         (map (comp str :file))
         (concat-arrow-files {:process-fn process-fn})))

  (require '[tech.v3.dataset.reductions :as ds-reduce])
  (require '[tech.v3.dataset.rolling :refer [rolling] :as ds-roll])
  (require '[ham-fisted.lazy-noncaching :as hfnc])
  (require '[ham-fisted.lazy-caching :as cfln])
  (let [btc (arrow/stream->dataset-seq
             "./btc-2025-03-10__08_25_53.arrow"
             {:key-fn keyword})]
    #_(-> (ds-reduce/aggregate
           {:n-elems (ds-reduce/row-count)
            :price-avg (ds-reduce/mean :px)
            :price-sum (ds-reduce/sum :px)
            :price-med (ds-reduce/prob-median :px)
            :price-iqr (ds-reduce/prob-interquartile-range :px)
            :n-dates (ds-reduce/count-distinct :tid :int32)}
           btc)
          clojure.pprint/pprint)
    (-> (hfnc/map (fn [ds]
                    (ds/sort-by-column ds :time <))
                  btc)
        first
        (ds/sort-by-column :time)
        ds/concat
        clojure.pprint/pprint))

  
  
  ,)
