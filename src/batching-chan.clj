(ns batching-chan
  (:require [clojure.core.async.impl.protocols :as impl]
            [clojure.core.async :as a :refer [chan go go-loop poll! <! >! alts!]]
            [tech.v3.dataset :as ds]
            [tech.v3.libs.arrow :as arrow])
  (:import (clojure.core.async.impl.channels ManyToManyChannel)
           (clojure.lang Counted)
           (java.util AbstractQueue)
           (java.util.concurrent PriorityBlockingQueue)))

(deftype BatchingBuffer [^PriorityBlockingQueue buf
                         ^ManyToManyChannel comm-ch
                         ^long batch-size
                         process-fn]
  impl/Buffer
  (full? [_]
    (>= (.size buf) batch-size))

  (remove! [this]
    (when (or (impl/full? this)
              (poll! comm-ch))
      (let [batch (java.util.ArrayList.)
            n (min batch-size (.size buf))]
        (loop [i 0]
          (when (< i n)
            (.add batch (.poll buf))
            (recur (inc i))))
        (process-fn (into [] batch))
        nil)))
  
  (add!* [this itm]
    (.add buf itm)
    this)

  (close-buf! [_]
    (when (pos? (.size buf))
      (process-fn (into [] buf))
      (.clear buf))
    (a/close! comm-ch))

  Counted
  (count [_]
    (.size buf))

  Object 
  (toString [this]
    (format "#BatchingBuffer{:batch-size: %d, :buf %s}"
            batch-size buf))

  clojure.lang.IDeref
  (deref [_]
    {:batch-size batch-size
     :buf (into [] buf)}))

(defn batching-chan
  "Creates a channel with a batching buffer that will accumulate items until either
  `batch-size` items have been received, or a timeout."
  [{:keys [batch-size timeout-ms comm-ch process-fn cmp-fn]}]
  (let [buffer (BatchingBuffer. 
                (PriorityBlockingQueue. 
                 (inc batch-size)
                 (comparator cmp-fn))
                comm-ch
                batch-size
                process-fn)
        ch (chan buffer)]
    (go-loop [timeout (a/timeout timeout-ms)]
      (let [[v port] (alts! [ch comm-ch timeout])]
        (cond
          (and (= port comm-ch) (= v :kill))
          (println "Closing!")
          
          (= port timeout)
          (do
            (>! comm-ch :flush)
            (recur (a/timeout timeout-ms)))
          
          (not (.closed? port))
          (do (.remove! buffer)
              (recur timeout))
          
          :else (recur timeout))))
    ch))

(comment
  (def comm-ch (chan (a/buffer 1)))
  (def batch-ch 
   (batching-chan
     {:batch-size 1000
      :timeout-ms 1000
      :comm-ch comm-ch
      :cmp-fn (fn [a b] (> (:prio a) (:prio b)))
      :process-fn (fn [items] (println 'Writing (ds/->dataset {:stuff items}) 'to "somewhere.arrow"))}))

  (def buf (.buf ^ManyToManyChannel batch-ch))
  (println buf)


  (poll! comm-ch)
  #_(go (println 'comm-ch-value (<! comm-ch)))
 
  (def putter-ch
    (go-loop []
      (<! (a/timeout (+ 500 (rand-int 1000))))
      (>! batch-ch {:t (System/currentTimeMillis)
                    :prio (rand-int 3)})
      (recur)))
  (go-loop []
    (when-not (.closed? comm-ch)
      (<! (a/timeout 100))
      (a/put! comm-ch :kill)
      (recur)))
  (a/close! putter-ch)
  (a/close! comm-ch)
  putter-ch
  (a/close! batch-ch)

  ,)
