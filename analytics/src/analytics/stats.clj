(ns analytics.stats (:require [util.math]))


(defn mode [vs]
  (let [fs (frequencies vs)]
    (first (last (sort-by second fs)))))

(defn quantile
  ([p vs]
     (let [svs (sort vs)]
       (quantile p (count vs) svs (first svs) (last svs))))
  ([p c svs mn mx]
     (let [pic (* p (inc c))
           k (int pic)
           d (- pic k)
           ndk (if (zero? k) mn (nth svs (dec k)))]
       (cond
        (zero? k) mn
        (= c (dec k)) mx
        (= c k) mx
        :else (+ ndk (* d (- (nth svs k) ndk)))))))

(defn median
  ([vs] (quantile 0.5 vs))
  ([sz svs mn mx] (quantile 0.5 sz svs mn mx)))

(defn mean
  ([vs] (mean (reduce + vs) (count vs)))
  ([sm sz] (/ sm sz)))

(defn standard-deviation-population
  ([vs]
     (standard-deviation-population vs (count vs) (mean vs)))
  ([vs sz u]
     (Math/sqrt (/ (reduce + (map #(Math/pow (- % u) 2) vs))
                   sz))))

(defn standard-deviation-sample
  ([vs]
     (standard-deviation-sample vs (count vs) (mean vs)))
  ([vs sz u]
     (Math/sqrt (/ (reduce + (map #(Math/pow (- % u) 2) vs))
                   (- sz 1)))))

(defn median-absolute-deviation
  ([vs]
     (median-absolute-deviation vs (median vs)))
  ([vs m]
     (median (map #(Math/abs (- % m)) vs))))

(defn lower-adjacent-value
  ([vs]
     (let [q1 (quantile 0.25 vs)
           m (median vs)
           q3 (quantile 0.75 vs)]
       (lower-adjacent-value (sort vs) m (- q3 q1))))
  ([svs m qd]
     (let [l (- m qd)]
       (first (filter (partial < l) svs)))))

(defn upper-adjacent-value
  ([vs]
     (let [q1 (quantile 0.25 vs)
           m (median vs)
           q3 (quantile 0.75 vs)]
       (upper-adjacent-value (reverse (sort vs)) m (- q3 q1))))
  ([rsvs m qd]
     (let [l (+ m qd)]
       (first (filter #(< % l) rsvs)))))

(defn stats-map
  ([vs]
     (let [sz (count vs)
           svs (sort vs)
           rsvs (reverse svs)
           mn (first svs)
           mx (first rsvs)
           sm (reduce + vs)
           u (mean sm sz)
           mdn (median sz svs mn mx)
           q1 (quantile 0.25 sz svs mn mx)
           q3 (quantile 0.75 sz svs mn mx)
           sd-population (standard-deviation-population vs sz u)
           sd-sample (standard-deviation-sample vs sz u)
           mad (median-absolute-deviation vs mdn)
           qd (- q3 q1)
           lav (lower-adjacent-value svs mdn qd)
           uav (upper-adjacent-value rsvs mdn qd)]
       {
        :size sz
        :min (util.math/round mn 2)
        :max (util.math/round mx 2)
        :mean (util.math/round u 2)
        :median (util.math/round mdn 2)
        :mode (util.math/round (mode vs) 2)
        :q1 q1
        :q3 q3
        :total sm
        :sd-population-1 (util.math/round sd-population 2)
        :sd-population-2 (util.math/round (* 2 sd-population) 2)
        :sd-population-4 (util.math/round (* 4 sd-population) 2)
        :sd-population-6 (util.math/round (* 6 sd-population) 2)
        :sd-sample-1 (util.math/round sd-sample 2)
        :sd-sample-2 (util.math/round (* 2 sd-sample) 2)
        :sd-sample-4 (util.math/round (* 4 sd-sample) 2)
        :sd-sample-6 (util.math/round (* 6 sd-sample) 2)
        :mad mad
        :lav lav
        :uav uav}))
  ([ks vs]
     (zipmap ks (map (stats-map vs) ks))))

(let [ks [:Size :Min :Max :Mean :Median :Mode :Q1 :Q3 :Total :SD :MAD :LAV :UAV]]
  (defn summarise
    ([vs] (summarise "" vs))
    ([label vs]
       (apply format
              (str (reduce #(.append %1 %2)
                           (StringBuilder.)
                           (interpose \tab
                                      ["%1$s::"
                                       "Size: %2$.3f"
                                       "Total: %10$.3f"
                                       "Mean: %5$.3f"
                                       "Mode: %7$.3f"
                                       "Min: %3$.3f"
                                       "LAV: %13$.3f"
                                       "Q1: %8$.3f"
                                       "Median: %6$.3f"
                                       "Q3: %9$.3f"
                                       "UAV: %14$.3f"
                                       "Max: %4$.3f"
                                       "SD: %11$.3f"
                                       "MAD: %12$.3f"])))
              (conj (map (comp double (stats-map vs)) ks) label)))))

(defn closest-mean-fn [means]
  (fn [v] (reduce (partial min-key #(Math/pow (- v %) 2)) means)))

(defn k-means [k vs]
  (let [vs (map double vs)
        svs (set vs)]
    (if (> k (count svs))
      (sort svs)
      (loop [mns (sort (take k (shuffle svs)))
             pmns (repeat k Double/NaN)]
        (if (= mns pmns)
          mns
          (recur (sort (map mean (vals (group-by (closest-mean-fn mns) vs)))) mns))))))
