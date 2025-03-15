(ns flr
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [ham-fisted.api :as hmf]))

(defn meet- [v1 v2] (dfn/min v1 v2))
(defn join- [v1 v2] (dfn/max v1 v2))

(defn- handle-missing-value [op v1 v2]
  (let [v1 (dtype/->reader v1)
        v2 (dtype/->reader v2)
        len (dtype/ecount v1)
        result-datatype (dtype/elemwise-datatype v1)
        result (dtype/make-container :java-array result-datatype len)]
    (dotimes [i len]
      (let [x1 (.readDouble v1 i)
            x2 (.readDouble v2 i)
            res-val (cond
                      (Double/isNaN x1) (case op
                                          :meet x2
                                          :join Double/NaN)
                      (Double/isNaN x2) (case op
                                          :meet x1
                                          :join Double/NaN)
                      :else (case op
                              :meet (min x1 x2)
                              :join (max x1 x2)))]
        (dtype/set-value! result i res-val)))
    result))

(defn comparable? [v1 v2]
  (let [meet-v1-v2 (meet- v1 v2)]
    (or (dfn/equals meet-v1-v2 v1)
        (dfn/equals meet-v1-v2 v2))))

(defn has-missing-values? [v]
  (->> (dtype/->reader v)
       (hmf/filterv nil?)
       hmf/empty?
       not))

(defn- split-last [v]
  ((juxt (comp dtype/->vector hmf/drop-last) hmf/last) v))

(defn join [v1 v2]
  (let [[a b] (split-last v1)
        [c d] (split-last v2)]
    (dtype/concat-buffers :float64 [(dfn/min a c) [(max b d)]])))

(defn meet [v1 v2]
  (let [[a b] (split-last v1)
        [c d] (split-last v2)]
    (dtype/concat-buffers :float64 [(dfn/max a c) [(min b d)]])))

(defn l1-distance [{x :min-point y :max-point :as _hyperbox}]
  (dfn/reduce-+ (dfn/abs (dfn/- x y))))

(defn l2-distance [{x :min-point y :max-point :as _hyperbox}]
  (dfn/distance x y))

(defn same-shape? [x y]
  (dfn/equals (dtype/shape x) (dtype/shape y)))

(defn ->hyperbox [min-point max-point class-label]
  {:min-point (dtype/->reader min-point :float64)
   :max-point (dtype/->reader max-point :float64)
   :class-label class-label})

(defn hyperbox-contains-point?
  [{:keys [min-point max-point] :as _hyperbox} point]
  (every? identity
          (map (fn [min-val point-val max-val]
                 (and (>= point-val min-val)
                      (<= point-val max-val)))
               min-point
               point
               max-point)))

(defn ->hyperbox-from-point [point class-label]
  (->hyperbox point point class-label))

(defn inclusion-measure [x u]
  (dfn// (dtype/ecount u)
         (dtype/ecount (join x u))))

(defn expand-hyperbox-to-point [hyperbox point]
  (-> hyperbox
      (update :min-point meet point)
      (update :max-point join point)))

(defn overlap
  [{min1 :min-point max1 :max-point :as _box1}
   {min2 :min-point max2 :max-point :as _box2}]
  (let [overlaps (mapv (fn [min1 max1 min2 max2]
                        (let [overlap-min (max min1 min2)
                              overlap-max (min max1 max2)]
                          (max 0.0 (- overlap-max overlap-min))))
                      min1 max1 min2 max2)]
    
    (if (some zero? overlaps)
      0.0
      (dfn/reduce-* overlaps))))

(defn volume [{:keys [min-point max-point] :as _hyperbox}]
  (dfn/reduce-* (dfn/abs (dfn/- min-point max-point))))

(defn normalize-box [hyperbox]
  (-> hyperbox
      (update :min-point dfn/normalize)
      (update :max-point dfn/normalize)))

(defn- fn-valuation
  [{:keys [min-point max-point]} pos-valuation-fn iso-valuation-fn]
  (let [[a b] (split-last min-point)
        [c d] (split-last max-point)]
    (+ (dfn/reduce-+
        (dtype/concat-buffers
         :float64
         [(dtype/emap iso-valuation-fn :float64 a)
          (dtype/emap iso-valuation-fn :float64 c)
          (dtype/emap pos-valuation-fn :float64 [b d])])))))

(defn linear-valuation [hyperbox]
  (fn-valuation hyperbox identity #(- 1 %) ))

(defn- sigmoid
  ([x]
   (sigmoid {:slope 5
             :x0 0.5} x))
  ([{:keys [slope x0]} x]
   (/ 1.0 (+ 1.0 (Math/exp (* (- slope) (- x x0)))))))

(defn sigmoid-valuation [hyperbox]
  (let [x0 0.4]
    (fn-valuation hyperbox
                  (partial sigmoid {:slope 6 :x0 x0})
                  #(- (* 2 x0) %))))

;; section 3 https://www.athanasiadis.info/assets/pdf/ijar2007.pdf
(let [x (->hyperbox [0.1 0.2] [0.3 0.4] :b1)
      u (->hyperbox [0.4 0.7] [0.2 0.5] :b1)
      w (->hyperbox [0.4 0.8] [0.2 0.7] :b1)]
  (prn (/ (sigmoid-valuation w)
          (sigmoid-valuation
           (->hyperbox
            (join (:min-point x) (:min-point w))
            (join (:max-point x) (:max-point w))
            :b1))))
  (prn (/ (sigmoid-valuation u)
          (sigmoid-valuation
           (->hyperbox
            (join (:min-point x) (:min-point u))
            (join (:max-point x) (:max-point u))
            :b1)))))
