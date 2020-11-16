(ns util.unixtime (:require
                   [clojure.tools.namespace.repl :refer [refresh]]
                   [java-time]
                   )
    )

(def day-ms 86400000)
(def hour-ms 3600000)
(def minute-ms 60000)
(def second-ms 1000)


(defn timestamp [] (System/currentTimeMillis))
(defn to-zoned-date-time [timestamp timezone] (java-time/zoned-date-time (java-time/instant timestamp) timezone))
(defn from-zoned-date-time [time] (inst-ms (java-time/instant time)))
(defn to-offset-date-time [timestamp offset] (java-time/zoned-date-time (java-time/instant timestamp) offset))
(defn from-offset-date-time [time] (inst-ms (java-time/instant time)))

(defn overlaps? [[from to] targets] "All parameters are unix timestamps. targets = vector of [from to] vectors. Returns true if source overlaps at least one target"
  (reduce (fn [overlapped [target-from target-to]]
            (if overlapped true ;keep returning true
                (if (or
                     (and (>= from target-from) (<= from target-to))
                     (and (>= to target-from) (<= from target-to))) true false)))
          false targets))

(defn days-in-range [from to] "Calculate number of days based on ms input" (/ (- to from) day-ms))

(defn str-date-time [timestamp] (java-time/format "yyyy-MM-dd HH:mm:ss:SSS" (to-zoned-date-time timestamp "Europe/Oslo")))
(defn str-date [timestamp] (java-time/format "yyyy-MM-dd" (to-zoned-date-time timestamp "Europe/Oslo")))
