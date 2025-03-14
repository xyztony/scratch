(ns ws-flow-ex
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as flow]
            [clojure.core.match :refer [match]]
            [hato.websocket :as ws]
            [charred.api :as json]
            [clj-commons.byte-streams :as bs]
            [utils :as utils]
            [tech.v3.dataset :as ds]
            [medley.core :as medley]
            [tech.v3.libs.arrow :as arrow]
            [tick.core :as t]))

(defn create-websocket [uri config]
  (try
    (ws/websocket uri config)
    (catch Exception e
      (println "Failed to create websocket:" (.getMessage e))
      nil)))

(defn create-pool [size]
  (fn [uri base-config]
    (vec (repeatedly size #(delay (create-websocket uri base-config))))))

(defn try-next-conn [config uri conn-pool]
  (try
    (if-let [next-conn (first conn-pool)]
      @(force next-conn)
      @(create-websocket uri config))
    (catch Exception _ nil)))

(defn- restart-active-conn [{:keys [active-conn conn-pool uri config] :as state}]
  (let [_ (when active-conn 
            (try (ws/close! active-conn) (catch Exception _)))
        new-conn (try-next-conn config uri conn-pool)
        new-pool (utils/next-or-empty conn-pool)

        replenished-pool (conj new-pool (delay (create-websocket uri config)))]
    [(assoc state 
            :active-conn new-conn
            :conn-pool replenished-pool
            :reconnect-pending false
            :retry-count 0)
     {:out [(if new-conn
              {:type :connection-restarted}
              {:type :connection-restart-failed})]}]))

(defn- close-active-conn [{:keys [active-conn] :as state}]
  (if active-conn 
               (try (ws/close! active-conn)
                    [(assoc state :active-conn nil) {:out [{:type :connection-closed-by-user}]}]
                    (catch Exception e
                      (prn "There was an error closing the active connection." state e)
                      [state]))
               [state]))

(defn- replenish-pool [{:keys [config uri conn-pool] :as state}]
  (let [pool-fn (create-pool (count conn-pool))
                   new-pool (pool-fn uri config)]
    [(assoc state :conn-pool new-pool) {:out [{:type :pool-replenished
                                               :pool-size (count new-pool)}]}]))

(defn- handle-commands
  [{:keys [active-conn conn-pool uri config auto-reconnect 
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
        (let [new-conn (try-next-conn config uri conn-pool)
              new-pool (utils/next-or-empty conn-pool)
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
             [:restart] (restart-active-conn state)
             [:close] (close-active-conn state)             
             [:replenish-pool] (replenish-pool state)
             [:status] [state {:out [{:type :status
                                      :active-connection? (boolean active-conn)
                                      :pool-size (count conn-pool)
                                      :auto-reconnect auto-reconnect
                                      :retry-count retry-count}]}]
             [:set-auto-reconnect] [(assoc state :auto-reconnect (boolean (:value msg))) 
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
                          @(create-websocket uri merged-config)
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

(defn ingest-transform
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
           :args {:max-dataset-size 1000}}
          
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

  (flow/ping ws-flow)
  
  (flow/pause ws-flow)
  
  (utils/monitoring (flow/start ws-flow))
  (flow/resume ws-flow)
 
  (send-command! ws-flow :restart)
  (send-command! ws-flow :close)
  (send-command! ws-flow :replenish-pool)
  (send-command! ws-flow :status)
  
  (flow/inject ws-flow
               [:pool-controller :control]
               [{:command :set-auto-reconnect :value false}])

  (->> (flow/ping-proc ws-flow :pool-controller)
       ::flow/state)

  (require '[tech.v3.libs.arrow :as arrow])

  (get-in (flow/ping-proc ws-flow :ingestion) [::flow/state :current-dataset])
  
  (-> (->> (get-in (flow/ping-proc ws-flow :ingestion) [::flow/state :current-dataset])
      concat
      (filter #(= "trades" (get % :channel)))
      (map #(first (get % :data)))
      (ds/->>dataset {:key-fn keyword
                      :parser-fn {:px :float32
                                  :sz :float64}}))
      (ds/select-columns [:px :sz :side :time :hash :tid])
      (arrow/dataset->stream! (format "btc-%s.arrow" (t/format (t/formatter "YYYY-MM-dd__hh_mm_ss") (t/date-time)))))

  (arrow/stream->dataset "btc-2025-03-09__03_08_37.arrow" {:text-as-strings? true :open-type :mmap})
    
  (flow/pause-proc ws-flow :ingestion)
  (flow/resume-proc ws-flow :ingestion)
  
  (flow/stop ws-flow)
  ,)



