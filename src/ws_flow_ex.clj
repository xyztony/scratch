(ns ws-flow-ex
  (:require [clojure.core.async :as a]
            [clojure.core.async.flow :as flow]
            [clojure.core.match :refer [match]]
            [hato.websocket :as ws]
            [charred.api :as json]
            [clojure.pprint :as pp]
            [clj-commons.byte-streams :as bs]
            [utils]))

(defn create-websocket [uri config]
  (try
    (ws/websocket uri config)
    (catch Exception e
      (println "Failed to create websocket:" (.getMessage e))
      nil)))

(defn create-pool [size]
  (fn [uri base-config]
    (vec (repeatedly size #(delay (create-websocket uri base-config))))))

(defn connection-pool-process
  "Main process managing the websocket pool"
  [uri symbol pool-size base-config]
  (flow/process
   {:describe
    (fn []
      {:outs {:out "Channel where messages are put"}
       :workload :io
       :params {:uri "Websocket URI"
                :symbol "Trading symbol"
                :pool-size "Size of connection pool"
                :base-config "Base websocket configuration"}})

    :init
    (fn [{:keys [uri symbol pool-size base-config]}]
      (let [msg-ch (a/chan 1024)
            base-ws-config
            {:on-open (fn [ws]
                        (prn "Subscribing " symbol)
                        (ws/send! ws
                                  (json/write-json-str
                                   {:method :subscribe
                                    :subscription {:type :trades
                                                   :coin symbol}})))
             :on-message (fn [ws msg _]
                           (prn "WS MSG: " msg)
                           (let [j (-> ^java.nio.HeapCharBuffer msg
                                       .array
                                       utils/read-json)]
                             (prn "WS Msg: " j)
                             (a/put! msg-ch j)))
             :on-close (fn [ws status reason]
                         (ws/send! ws
                                   (json/write-json-str
                                    {:method :unsubscribe
                                     :subscription {:type :trades
                                                    :coin symbol}}))
                         (a/put! msg-ch {:type :connection-closed
                                         :status status
                                         :reason reason}))
             :on-error (fn [ws err]
                         (a/put! msg-ch {:type :connection-error
                                         :error err}))}
            merged-config (merge base-config base-ws-config)]
          
        {:pool ((create-pool pool-size) uri base-config)
         :active-conn @(create-websocket uri base-config)
         :config merged-config
         ::flow/in-ports {:in-messages msg-ch}}))

    :transform
    (fn [state in-name msg]
      (prn state in-name msg)
      (if (= :in-messages in-name)
        [state {:out [msg]}]
        [state]))}))

(defn create-websocket-flow
  [{:keys [uri symbol pool-size base-config]}]
  (let [flow-def
        {:procs
         {:pool-controller
          {:proc (connection-pool-process uri symbol pool-size base-config)
           :args {:uri uri
                  :symbol symbol
                  :pool-size pool-size
                  :base-config base-config}}
          
          :message-handler
          {:proc (flow/process
                  {:describe (fn [] {:ins {:in "Incoming messages"}})
                   :transform (fn [_ _ msg] (prn "Received:" msg))})}}

         :conns
         [[[:pool-controller :out] [:message-handler :in]]]}]
    
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
      :pool-size 0
      :base-config {}}))

  
  (flow/pause ws-flow)

  
  (monitoring (flow/start ws-flow))
  (flow/resume ws-flow)

  (send-command! ws-flow :connect)
  (send-command! ws-flow :disconnect)
  (send-command! ws-flow :connect)
  
  (flow/ping ws-flow)
    
  (flow/stop ws-flow)
  ,)
