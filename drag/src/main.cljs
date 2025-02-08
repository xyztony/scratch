(ns main
  (:require [replicant.dom :as r]
            [clojure.core.async :as a]
            [clojure.walk :as walk]))

(def date-chan (a/chan (a/sliding-buffer 1)))
(def counter-chan (a/chan (a/sliding-buffer 1)))
(def main-chan (a/chan (a/sliding-buffer 1)))

(defn render [{:keys [message count] :as state}]
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
       [:p (str "Clicked " count " times!")]])]])

(defonce el (js/document.getElementById "app"))

(defn interpolate-actions [event actions]
  (walk/postwalk
   (fn [x]
     (case x
       :event/target.value (.. event -target -value)
       x))
   actions))

(defn execute-actions [actions]
  (doseq [[action & args] actions]
    (case action
      :store/assoc-in (a/put! counter-chan :inc)
      :store/decr-in (a/put! counter-chan :dec)
      :store/reset (a/put! counter-chan :reset)
      (println "Unknown action"))))

(defn ^:dev/after-load main []
  (a/go-loop []
    (a/<! (a/timeout 1000))
    (a/>! date-chan (js/Date.))
    (recur))

  (a/go-loop [date nil
              count 0]
    (let [[v port] (a/alts! [date-chan counter-chan])]
      (cond
        (= port date-chan)
        (do (a/>! main-chan {:message v :count count})
            (recur v count))
        
        (= port counter-chan)
        (let [count
              (case v
                :inc (inc count)
                :dec (dec count)
                :reset 0)]
          (a/>! main-chan {:message date :count count})
          (recur date count))
        
        :else (recur date count))))
  
  (a/go-loop []
    (r/render el (render (a/<! main-chan)))
    (recur))

  (r/set-dispatch!
   (fn [event-data actions]
     (->> actions
          (interpolate-actions
           (:replicant/dom-event event-data))
          (execute-actions)))))
