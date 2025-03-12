(ns arrow-utils
  (:require
   [tech.v3.libs.arrow :as arrow]
   [tech.v3.dataset :as ds]
   [tech.v3.libs.guava.cache :as cache]
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
   :key-fn keyword
   :resource-type :gc})

(defn file-extension [path]
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


(defonce ^:private arrow-file-buffer (atom {}))

(defn add-file-to-buffer
  ([path]
   (add-file-to-buffer path path {}))
  ([path key]
   (add-file-to-buffer path key {}))
  ([path key options]
   (let [opts (merge default-options options)
         datasets (arrow/stream->dataset-seq path opts)]
     (swap! arrow-file-buffer assoc key
            {:path path
             :options opts
             :datasets datasets
             :added-at (System/currentTimeMillis)})
     key)))

(defn get-file-from-buffer [key] (get @arrow-file-buffer key))
(defn current-buffered-files [] (keys @arrow-file-buffer))
(defn clear-buffer [] (reset! arrow-file-buffer {}))
(defn remove-file-from-buffer [key]
  (when (get @arrow-file-buffer key)
    (swap! arrow-file-buffer dissoc key)
    true))

(defn concat-arrow-files
  [{:keys [process-fn arrow] :as _opts} paths]
  (let [opts (merge default-options arrow)]
    (->> paths
         (mapcat #(arrow/stream->dataset-seq % opts))
         (hfnc/map process-fn)
         (apply ds/concat))))

(defn load-ticker-files [dir ticker ext & [options]]
  (let [pattern (ticker-yyyy-mm-dd__hh_mm_ss-pattern ticker ext)
        files-with-matches
        (->> (file-seq (io/file dir))
             (keep (partial file->pattern-matches pattern)))]
    (->> files-with-matches
         (sort-by :matches date-match-comp)
         (map (comp str :file))
         (run! #(add-file-to-buffer % % options)))))

(defn- memoize-with-ttl [afn ttl-seconds]
  (cache/memoize afn
   {:access-ttl-ms (t/millis (t/of-seconds ttl-seconds))}))

(defn- caching-ds-run [key ds-fn]
  (fn [ds]
    (let [result (ds-fn ds)
          fn-key (gensym "caching-ds-run-")
          memoized-fn (memoize-with-ttl ds-fn 30)]
      (swap! arrow-file-buffer                                                               update-in [key :cache-fns]
             (fnil conj []) {:id fn-key :fn memoized-fn})
      result)))

(defn get-cached-fn-result [key fn-key]
  (some->> (get-in @arrow-file-buffer [key :cache-fns])
           (medley/find-first #(= fn-key (keyword (:id %))))
           :fn))

(defn run-fn-on-buffer! [afn]
  (dorun 
   (->> @arrow-file-buffer
        (medley/map-kv (fn [key {:keys [datasets]}]
                         (hfnc/map (caching-ds-run key afn) datasets))))))

(comment
  (load-ticker-files "." "btc" "arrow")
  
  (vals @arrow-file-buffer)

  (run-fn-on-buffer! (fn [ds]
                       (ds/sort-by-column ds :time >)))

  (->> (get-in @arrow-file-buffer ["./btc-2025-03-09__03_06_54.arrow" :cache-fns])
       (medley/find-first #(not (nil? (:id %))))
       :id)
      
  (clear-buffer)
  ,)
