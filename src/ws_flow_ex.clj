(ns ws-flow-ex
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as flow]
            [clojure.core.match :refer [match]]
            [hato.websocket :as ws]
            [charred.api :as json]
            [clojure.pprint :as pp]
            [clj-commons.byte-streams :as bs]
            [utils]))

(defn- next-or-empty [coll]
  (or (and (seq coll) (next coll)) []))

(defn create-websocket [uri config]
  (try
    (ws/websocket uri config)
    (catch Exception e
      (println "Failed to create websocket:" (.getMessage e))
      nil)))

(defn create-pool [size]
  (fn [uri base-config]
    (vec (repeatedly size #(delay (create-websocket uri base-config))))))

(defn- handle-commands
  [{:keys [active-conn conn-pool uri config symbol auto-reconnect 
           reconnect-delay-ms reconnect-pending retry-count] :as state}
   in-name
   {msg-type :type :as msg}]
  (cond
    (= :messages in-name)
    (let [is-error-or-close (or (= :connection-error msg-type) 
                                (= :connection-closed msg-type))]
      (cond
        (and is-error-or-close auto-reconnect (not reconnect-pending))
        (do
          (a/go
            (a/<! (a/timeout reconnect-delay-ms))
            (a/>! (get-in state [::flow/in-ports :messages]) 
                  {:type :auto-reconnect-trigger}))
          [(assoc state :reconnect-pending true) {:out [msg]}])
        
        (= :connection-opened msg-type)
        [(assoc state :retry-count 0) {:out [msg]}]

        (= :auto-reconnect-trigger msg-type)
        (let [new-conn (try
                         (if-let [next-conn (first conn-pool)]
                           (force next-conn)
                           (create-websocket uri config))
                         (catch Exception e
                           nil))
              new-pool (next-or-empty conn-pool)
              replenished-pool (conj new-pool (delay (create-websocket uri config)))
              new-retry-count (inc retry-count)]
          
          (if new-conn
            [(assoc state 
                    :active-conn new-conn
                    :conn-pool replenished-pool
                    :reconnect-pending false
                    :retry-count 0)
             {:out [{:type :connection-auto-restarted}]}]
            
            ;; Failed reconnection, schedule another attempt with backoff
            (do
              (when auto-reconnect
                (a/go
                  (let [backoff-ms (* reconnect-delay-ms (min 10 new-retry-count))]
                    (a/<! (a/timeout backoff-ms))
                    (a/>! (get-in state [::flow/in-ports :messages]) 
                          {:type :auto-reconnect-trigger}))))
              [(assoc state 
                      :conn-pool replenished-pool
                      :retry-count new-retry-count)
               {:out [{:type :connection-auto-restart-failed
                       :retry-count new-retry-count}]}])))
        
        :else
        [state {:out [msg]}]))
    
    ;; User commands
    (= :control in-name)
    (let [command (:command msg)]
      (match [command]
             [:restart] 
             (let [_ (when active-conn 
                       (try (ws/close! active-conn) (catch Exception _)))
                   new-conn (try
                              (if-let [next-conn (first conn-pool)]
                                (force next-conn)
                                (create-websocket uri config))
                              (catch Exception e
                                nil))
                   new-pool (next-or-empty conn-pool)

                   replenished-pool (conj new-pool (delay (create-websocket uri config)))]
               [(assoc state 
                       :active-conn new-conn
                       :conn-pool replenished-pool
                       :reconnect-pending false
                       :retry-count 0)
                {:out [(if new-conn
                         {:type :connection-restarted}
                         {:type :connection-restart-failed})]}])
             
             [:close]
             (do
               (when active-conn 
                 (try (ws/close! active-conn) (catch Exception _)))
               [(assoc state :active-conn nil) {:out [{:type :connection-closed-by-user}]}])
             
             [:replenish-pool]
             (let [pool-fn (create-pool (count conn-pool))
                   new-pool (pool-fn uri config)]
               [(assoc state :conn-pool new-pool) {:out [{:type :pool-replenished
                                                          :pool-size (count new-pool)}]}])
             
             [:status]
             [state {:out [{:type :status
                            :active-connection? (boolean active-conn)
                            :pool-size (count conn-pool)
                            :auto-reconnect auto-reconnect
                            :retry-count retry-count}]}]
             
             [:set-auto-reconnect]
             [(assoc state :auto-reconnect (boolean (:value msg))) 
              {:out [{:type :auto-reconnect-updated
                      :enabled (boolean (:value msg))}]}]
             
             :else
             [state {:out [{:type :unknown-command
                            :command command}]}]))
    
    :else
    [state]))

(defn connection-pool-process
  "Main process managing the websocket pool"
  []
  (flow/process
   {:describe
    (fn []
      {:ins {:control "Channel for control commands"}
       :outs {:out "Channel where messages are put"}
       :workload :io
       :params {:uri "Websocket URI"
                :symbol "Trading symbol"
                :base-config "Base websocket configuration"
                :pool-size "Size of connection pool (default: 3)"
                :auto-reconnect "Auto reconnect on failure (default: true)"
                :reconnect-delay-ms "Delay before reconnection (default: 1000)"}})

    :init
    (fn [{:keys [uri symbol base-config pool-size auto-reconnect reconnect-delay-ms] :as args}]
      (let [pool-size (or pool-size 3)
            auto-reconnect (if (nil? auto-reconnect) true auto-reconnect)
            reconnect-delay-ms (or reconnect-delay-ms 1000)
            msg-ch (a/chan 1024)
            base-ws-config
            {:on-open (fn [ws]
                        (a/put! msg-ch {:type :connection-opened})
                        (ws/send! ws (json/write-json-str
                                      {:method :subscribe
                                       :subscription {:type :trades
                                                      :coin symbol}})))
             :on-message (fn [_ws msg _]
                           (let [j (-> msg bs/to-string utils/read-json)]
                             (a/put! msg-ch j)))
             :on-close (fn [ws status reason]
                         (ws/send! ws (json/write-json-str
                                       {:method :unsubscribe
                                        :subscription {:type :trades
                                                       :coin symbol}}))
                         (a/put! msg-ch {:type :connection-closed
                                         :status status
                                         :reason reason}))
             :on-error (fn [_ws err]
                         (a/put! msg-ch {:type :connection-error
                                         :error (str err)}))}
            merged-config (merge base-config base-ws-config)
            pool-fn (create-pool pool-size)
            conn-pool (pool-fn uri merged-config)
            active-conn (try
                          (create-websocket uri merged-config)
                          (catch Exception e
                            (a/put! msg-ch {:type :connection-error
                                            :phase :initial
                                            :error (str e)})
                            nil))]
          
        {:active-conn active-conn
         :conn-pool conn-pool
         :config merged-config
         :uri uri
         :symbol symbol
         :auto-reconnect auto-reconnect
         :reconnect-delay-ms reconnect-delay-ms
         :reconnect-pending false
         :retry-count 0
         ::flow/in-ports {:messages msg-ch}}))

    :transition
    (fn [{:keys [active-conn] :as state} transition]
      (when (= transition ::flow/stop)
        (when active-conn
          (try
            (ws/close! active-conn)
            (catch Exception _))))
      state)

    :transform handle-commands}))

(defn ^:dynamic ingest-transform
  [{:keys [current-dataset max-dataset-size dropped] :as state} in-name msg]
  (if (= :in in-name)
    (if (nil? msg)
      [state]
      (let [updated-dataset (conj current-dataset msg)
            row-count (count updated-dataset)
            trimmed-dataset (if (> row-count max-dataset-size)
                              (drop max-dataset-size updated-dataset)
                              updated-dataset)]
        (when (> row-count max-dataset-size) (prn "Dropped: " max-dataset-size "prev count: " row-count "time dropped: " dropped))
        [(assoc state
                :current-dataset trimmed-dataset
                :dropped (if (> row-count max-dataset-size) (inc dropped) dropped))]))
    [state]))

(defn ingestion-process
  "Process for ingesting data into a dataset."
  []
  (flow/process
   {:describe
    (fn []
      {:ins {:in "Channel for incoming data"
             :control "Channel for control commands"}
       :outs {:dataset-updates "Channel for dataset updates"
              ;; TODO snapshots wip
              :snapshot-trigger "Channel to trigger dataset snapshots"}
       :params {:max-dataset-size "Maximum number of rows in working dataset (default: 100000)"
                :snapshot-interval-ms "Interval between snapshots in ms (default: 300000 = 5 minutes)"}})

    :init
    (fn [{:keys [max-dataset-size snapshot-interval-ms] :as args}]
      (let [max-size (or max-dataset-size 100000)
            snapshot-interval (or snapshot-interval-ms 300000)
            current-dataset [] #_(create-empty-dataset)
            snapshot-ch (a/chan (a/sliding-buffer 1))]

        ;; TODO finish impl
        (a/go-loop []
          (a/<! (a/timeout snapshot-interval))
          (a/>! snapshot-ch {:type :time-based})
          (recur))
        
        {:current-dataset current-dataset
         :max-dataset-size max-size
         :dropped 0
         :last-snapshot-time (System/currentTimeMillis)
         ::flow/out-ports {:snapshot-timer snapshot-ch}}))

    :transition
    (fn [state transition]
      (when (= transition ::flow/stop)
        (when-let [timer-ch (get-in state [::flow/out-ports :snapshot-timer])]
          (a/close! timer-ch)))
      state)

    :transform ingest-transform}))

(def ingestion-process-handler ingestion-process)

(defn create-websocket-flow
  [{:keys [uri symbol base-config pool-size auto-reconnect reconnect-delay-ms]}]
  (let [flow-def
        {:procs
         {:pool-controller
          {:proc (connection-pool-process)
           :args {:uri uri
                  :symbol symbol
                  :base-config base-config
                  :pool-size pool-size
                  :auto-reconnect auto-reconnect
                  :reconnect-delay-ms reconnect-delay-ms}}
          
          :ingestion
          {:proc (ingestion-process)
           :args {:max-dataset-size 10}}
          
          :message-handler
          {:proc (flow/process
                  {:describe (fn [] {:ins {:in "Incoming messages"}
                                     :workload :io})
                   :transform (fn [state _ msg] 
                                (prn "Received message:" msg)
                                [state])})}}

         :conns
         [[[:pool-controller :out] [:message-handler :in]]
          [[:pool-controller :out] [:ingestion :in]]]}]
    
    (flow/create-flow flow-def)))

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

(defn send-command! [flow command]
  (flow/inject flow [:pool-controller :control] [{:command command}]))

(comment
  (def ws-flow
    (create-websocket-flow
     {:uri "wss://api.hyperliquid.xyz/ws"
      :symbol "BTC"
      :base-config {}
      :pool-size 3
      :auto-reconnect true
      :reconnect-delay-ms 1000}))

  (flow/pause ws-flow)
  
  (monitoring (flow/start ws-flow))
  (flow/resume ws-flow)
 
  (send-command! ws-flow :restart)
  (send-command! ws-flow :close)
  (send-command! ws-flow :replenish-pool)
  (send-command! ws-flow :status)
  
  (flow/inject ws-flow [:pool-controller :control] [{:command :set-auto-reconnect :value true}])
  
  (flow/pause-proc ws-flow :ingestion)
  
  (->> (flow/ping-proc ws-flow :ingestion)

       ::flow/state
       :dropped
       )

  (meta #'ingest-transform)
  
  (flow/stop ws-flow)
  ,)
