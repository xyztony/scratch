(ns main
  (:require [replicant.dom :as r]
            [clojure.core.async :as a]
            [clojure.string :refer [upper-case]]
            [clojure.walk :as walk]))


(def ws-url "wss://api.hyperliquid.xyz/ws")
(def ws-chan (a/chan (a/sliding-buffer 1)))
(def ws-control-chan (a/chan (a/sliding-buffer 1)))
(def ws-signal-chan (a/chan (a/sliding-buffer 1)))
(def coin-chan (a/chan (a/sliding-buffer 1)))

(def date-chan (a/chan (a/sliding-buffer 1)))
(def counter-chan (a/chan (a/sliding-buffer 1)))
(def main-chan (a/chan (a/sliding-buffer 1)))

(defn render-book-row [{:keys [px sz n]}]
  [:tr
   [:td px]
   [:td sz]
   [:td n]])

(defn render-book-table [{:keys [should-subscribe?]
                          {:keys [coin time bids asks]} :book}]
  [:div
   [:h2 (str (when-not should-subscribe? "[PAUSED] ")
             coin
             " Order Book @ " (js/Date. time))]
   [:div.flex
    [:table.bids
     [:thead
      [:tr
       [:th "Price"]
       [:th "Size"]
       [:th "Orders"]]]
     [:tbody
      (map render-book-row bids)]]
    [:table.asks
     [:thead
      [:tr
       [:th "Price"]
       [:th "Size"]
       [:th "Orders"]]]
     [:tbody
      (map render-book-row asks)]]]])

(defn extract-level-data [book]
  (when (= "l2Book" (:channel book))
    (let [data (:data book)
          coin (:coin data)
          time (:time data)
          levels (:levels data)]
      {:coin coin
       :time time
       :bids (->> (first levels)
                  (take 10)
                  vec
                  reverse)
       :asks (->> (second levels)
                  (take 10)
                  vec)})))

(defn render [{:keys [message count book should-subscribe? coin] :as state}]
  [:div
   [:h1 "Hello world"]
   [:p "Started at " message]
   [:div
    [:button {:on {:click [[:store/assoc-in]]}}
     '+]
    (when (pos? count)
      [:div 
       [:button {:on {:click [[:store/decr-in]]}}
        '-]
       [:button {:on {:click [[:store/reset]]}}
        'X]
       [:p (str "Clicked " count " times!")]])]
   [:div
    (when coin
      [:p (str "Current coin: " coin)])
    [:div
     [:label "Enter ticker:"]
     [:input {:type "text"
              :value (upper-case coin)
              :placeholder "Enter coin (e.g. BTC, ETH, SOL)"
              :on {:keydown [[:store/change-coin-keydown
                              {:value :coin-input/value
                               :input-key :coin-input/key}]]}}]]
    [:button
     {:on {:click [[:store/toggle-subscription
                    {:should-subscribe? (not should-subscribe?)}]]}}
     (if should-subscribe? "Unsubscribe" "Subscribe")]
    (when book
      (render-book-table state))]])

(defonce el (js/document.getElementById "app"))

(defn interpolate-actions [event actions]
  (walk/postwalk
   (fn [x]
     (case x
       :coin-input/value (.. event -target -value)
       :coin-input/key (.. event -key)
       x))
   actions))

(defn execute-actions [actions]
  (doseq [[action & args] actions]
    (case action
      :store/assoc-in (a/put! counter-chan :inc)
      
      :store/decr-in (a/put! counter-chan :dec)
      
      :store/reset (a/put! counter-chan :reset)
      
      :store/toggle-subscription
      (let [msg (:should-subscribe? (first args))]
        (a/put! ws-control-chan msg))

      :store/change-coin-keydown
      (when (= "Enter" (:input-key (first args)))
        (a/put! coin-chan (:value (first args))))
      
      (println "Unknown action"))))

(defn- ws-send [ws msg]
  (->> (clj->js msg)
       (js/JSON.stringify)
       (.send ws)))

(defn create-websocket []
  (let [ws (js/WebSocket. ws-url)]
    (set! (.-onopen ws)
          (fn [_]
            (ws-send ws {:method "subscribe"
                         :subscription {:type "l2Book"
                                        :coin "SOL"}})))
    
    (set! (.-onmessage ws)
          (fn [event]
            (let [data (-> event .-data js/JSON.parse (js->clj :keywordize-keys true))]
              (a/put! ws-chan data))))
    
    (set! (.-onerror ws)
          (fn [error]
            (js/console.error "WebSocket error:" error)))
    
    (set! (.-onclose ws)
          (fn [event]
            (ws-send ws {:method "unsubscribe"
                         :subscription {:type "l2Book"
                                        :coin "SOL"}})
            (js/console.log "WebSocket closed:" event)))
    
    ws))



(defn init! []
  (let [ws (create-websocket)]
    (a/go-loop []
      (a/<! (a/timeout 60000))
      (ws-send ws {:method "ping"})
      (recur))

    (a/go-loop []
      (let [{:keys [should-subscribe? coin prev-coin] :as state} (a/<! ws-signal-chan)]
        (if (not (= coin prev-coin))
          (do 
            (ws-send ws {:method "unsubscribe"
                         :subscription {:type "l2book"
                                        :coin prev-coin}})
            (and should-subscribe? 
                 (ws-send ws {:method "subscribe"
                              :subscription {:type "l2book"
                                             :coin coin}})))
          
          (ws-send ws {:method (if should-subscribe?
                                 "subscribe"
                                 "unsubscribe")
                       :subscription {:type "l2Book"
                                      :coin coin}})))
      (recur)))
  
  (a/put! date-chan (js/Date.)))

(defn ^:dev/after-load main []
  (init!)
  
  (a/go-loop []
    (a/<! (a/timeout 1000))
    (a/>! date-chan (js/Date.))
    (recur))

  (a/go-loop [state {:date nil
                     :count 0
                     :book {}
                     :should-subscribe? true
                     :coin "SOL"}]
    (let [[v port] (a/alts! [date-chan
                             counter-chan
                             ws-chan
                             ws-control-chan
                             coin-chan])
          
          update
          (cond (= port ws-chan)
                (when-let [book (extract-level-data v)]
                  {:book book})
                
                (= port date-chan)
                {:message v}
                
                (= port counter-chan)
                {:count
                 (case v
                   :inc (inc (:count state))
                   :dec (dec (:count state))
                   :reset 0)}

                (= port ws-control-chan)
                (do 
                  (a/put! ws-signal-chan
                          {:should-subscribe? v
                           :coin (:coin state)
                           :prev-coin (:coin state)})
                  {:should-subscribe? v})

                (= port coin-chan)
                (do
                  (a/put! ws-signal-chan
                          {:should-subscribe? (:should-subscribe? state)
                           :coin (upper-case v)
                           :prev-coin (:coin state)})
                  {:coin (upper-case v)}))
          
          state (merge state update)]
      (a/>! main-chan state)
      (recur state)))
  
  (a/go-loop []
    (r/render el (render (a/<! main-chan)))
    (recur))

  (r/set-dispatch!
   (fn [event-data actions]
     (->> actions
          (interpolate-actions
           (:replicant/dom-event event-data))
          (execute-actions)))))
