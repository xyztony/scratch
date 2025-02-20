(ns websocket-pool
  (:require [clojure.core.async :as a]
            [clojure.core.match :refer [match]]
            [clj-commons.byte-streams :as bs]
            [hato.websocket :as ws]
            [charred.api :as json]
            [utils :as utils]))

(defprotocol IWebsocketPool
  (start! [this])
  (stop! [this])
  (send! [this data])
  (get-status [this])
  (active-connection [this])
  (reconnect [this]))

(deftype WebsocketPool [uri
                        symbol
                        pool-size
                        ^:volatile-mutable active-conn
                        ^:volatile-mutable conn-pool
                        events-ch
                        status-ch
                        control-ch
                        config]
  IWebsocketPool
  
  (start! [this]
    (let [init-pool
          (vec
           (repeatedly
            pool-size
            #(delay
              (ws/websocket
               uri
               (merge
                {:on-open
                 (fn [ws]
                   (ws/send! ws
                             (json/write-json-str
                              {:method :subscribe
                               :subscription {:type :trades
                                              :coin symbol}}))
                   
                   (a/go-loop [timer (a/timeout 60000)]
                     (a/alt!
                       [control-ch timer]
                       ([v p]
                        (match [v p]
                          [_ timer]
                          (do (ws/ping! ws (-> (json/write-json-str {:method :ping})
                                               (bs/convert java.nio.ByteBuffer)))
                            (recur (a/timeout 60000)))
                         
                          [:close control-ch]
                          (ws/close! ws)
                         
                          :else (recur timer))))))

                 :on-message (fn [ws msg _]
                               (let [j (-> ^java.nio.HeapCharBuffer msg
                                           .array
                                           utils/read-json)]
                                 (a/put! events-ch j)))
                 :on-close (fn [ws status reason]
                             (ws/send! ws
                                       (json/write-json-str
                                        {:method :unsubscribe
                                         :subscription {:type :trades
                                                        :coin symbol}})))

                 :on-error (fn [ws err]
                             (println "WS ERROR: " err)
                             (a/put! control-ch {:type :error
                                                 :message err
                                                 :ws ws}))}
                config)))))]
      (set! active-conn @(first init-pool))
      (set! conn-pool (next init-pool))
      (a/put! status-ch {:status :connected
                         :conn active-conn})
      
      (a/go-loop []
        (let [[event _] (a/alts! [events-ch control-ch])]
          (case (:type event)
            :restart (.reconnect this)
            :error (when (= (:ws event) active-conn)
                     (.reconnect this))
            :closed (when (= (:ws event) active-conn)
                      (.reconnect this))
            nil))
        (when (a/<! control-ch)
          (recur)))
      this))
  
  (stop! [this]
    (when @active-conn
      (ws/close! @active-conn))
    (doseq [conn conn-pool]
      (when (realized? conn)
        (ws/close! @conn)))
    this)
  
  (send! [this data]
    (when active-conn
      (ws/send! active-conn data)))
  
  (get-status [this]
    (a/poll! status-ch))
  
  (active-connection [this]
    active-conn)
  
  (reconnect [this]
    (let [[new-conn idx] (loop [remaining-pool conn-pool
                                retry-idx 0]
                           (when-let [next-conn (first remaining-pool)]
                             (if-let [conn (try 
                                             @next-conn
                                             (catch Exception _
                                               (a/<!! (a/timeout
                                                       (min 30000
                                                            (* 1000 (Math/pow 2 retry-idx)))))))]
                               [conn retry-idx]
                               (recur (rest remaining-pool) (inc retry-idx)))))]
      (when new-conn
        (set! active-conn new-conn)
        (set! conn-pool (drop (inc idx) conn-pool))
        (a/put! status-ch {:status :reconnected
                           :conn new-conn})))))

(defn create-websocket-pool
  [uri symbol base-config pool-size {:keys [events-ch status-ch control-ch] :as _channels}]
  (->WebsocketPool uri
                   symbol
                   pool-size
                   nil
                   []
                   events-ch
                   status-ch
                   control-ch
                   base-config))


(comment

  (def btc-events-ch (a/chan (a/sliding-buffer 100)))
  (def btc-status-ch (a/chan (a/sliding-buffer 10)))
  (def btc-control-ch (a/chan (a/sliding-buffer 10)))
  (def btc-ws (create-websocket-pool "wss://api.hyperliquid.xyz/ws" "BTC"
                                     {} 3
                                     {:events-ch btc-events-ch
                                      :status-ch btc-status-ch
                                      :control-ch btc-control-ch}))
    
  (a/go-loop []
    (when-not @(.closed btc-events-ch)
      (println (a/<! btc-events-ch))
      (recur)))
  (start! btc-ws)
  (stop! btc-ws)
  
  (a/poll! btc-status-ch)
  (a/poll! btc-events-ch)
  @(.closed btc-events-ch)

  (get-status btc-ws)

  (reconnect btc-ws)

  btc-ws
    
  (a/put! btc-control-ch :close)
  
  ,)

