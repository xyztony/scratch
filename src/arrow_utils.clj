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
     (arrow/stream->dataset path opts)))

(comment
  
  ,)
