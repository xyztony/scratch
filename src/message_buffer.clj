(ns message-buffer
  (:require
   [clojure.core.async :as a]
   [tech.v3.resource :as resource]
   [tick.core :as t]))


(defrecord MessageBuffer [pending-batches
                          input-chan
                          flush-chan
                          overflow-chan
                          buffer-config])

(defn- flush!!
  [{:keys [pending-batches] :as _buffer}]
  (let [batches @pending-batches]
    (prn "!!FLUSH!!")
    (reset! pending-batches {})
    (a/thread
      (resource/releasing!
        (prn "Writing: " batches)))))

(defn- start-message-processor!
  "Starts the main loop that processes incoming messages"
  [{:keys [input-chan flush-chan overflow-chan pending-batches buffer-config] :as buffer}]
  (a/go-loop []
    (a/alt!
      input-chan
      ([message]
       (if (nil? message)
         (flush!! buffer)
         
         (let [ts-key (->> message
                           :timestamp
                           ((:time-partition-fn buffer-config))
                           ((:time-format-fn buffer-config)))
               
               updated-map (swap! pending-batches update ts-key 
                                  (fn [msgs] (conj (or msgs []) message)))
               
               batch (get updated-map ts-key)
               batch-ready? (>= (count batch) (:batch-size buffer-config))
               
               flush? (> (count updated-map) (:max-pending-batches buffer-config))]
           
           (cond
             batch-ready? 
             (do
               (prn "Sending ready batch: " batch)
               (swap! pending-batches dissoc ts-key))
             
             flush?
             (a/put! overflow-chan true)
             
             :else nil)
           (recur))))
      
      flush-chan
      ([_]
       (prn "Scheduling buffer " buffer)
       (recur))
      
      (a/timeout 5000)
      ([_]
       (when (not-empty @pending-batches)
         (prn "Scheduling timed out buffer " buffer)
         (reset! pending-batches {}))
       (recur)))))

(defn create-buffer [options]
  (let [config (merge 
                {:primary-buffer-size 10000
                 :batch-size 1000
                 :max-pending-batches 100
                 :time-partition-fn #(t/truncate % :minutes)
                 :time-format-fn #(t/format (t/formatter "yyyy-MMM-dd-HH-mm") %)
                 :writer-pool-size 10}
                options)
        
        input-chan (a/chan (:primary-buffer-size config))
        flush-chan (a/chan)
        overflow-chan (a/chan)
        
        buffer (->MessageBuffer 
                (atom {}) input-chan flush-chan overflow-chan config)]
    
    (start-message-processor! buffer)
    
    
    buffer))

(comment
  (defn put-message! [buffer message]
    (a/put! (:input-chan buffer) message))

  (defn force-flush! [buffer]
    (a/put! (:flush-chan buffer) true))

  (def demo-buffer 
    (create-buffer {:primary-buffer-size 10
                    :batch-size 5}))
  
  (def fake-data-loop
    (a/go-loop []
      (a/<! (a/timeout 1000))
      (let [some-data
            {:timestamp (t/date-time)
             :some-num (rand 1000)
             :some-range (range (rand-int 100))}]
        (a/>! (:input-chan demo-buffer) some-data))
      (recur)))
  
  (defn- close-chans! []
    (a/close! (:input-chan demo-buffer))
    (a/close! (:flush-chan demo-buffer))
    (a/close! (:overflow-chan demo-buffer))
    (a/close! fake-data-loop))

  (force-flush! demo-buffer)
  (close-chans!)
  ,)
