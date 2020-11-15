(ns pruning.core (:require
                  [command.dispatch :as d]
                  [event-store.storage :as s]
                  [util.uuid :as u]
                  [util.unixtime :as ut]
                  [query.all-vessels :as qv]
                  [clojure.tools.namespace.repl :refer [refresh]]
                  [util.benchmarking :as b]
                           ))


(def mysql {:type :mysql
                       :name "event_store"
                       :user "event_store"
                       :password "password"
                       :host "localhost"
                       :port 3306})

(def mysql-projections {:type :mysql
                       :name "event_store"
                       :user "event_store"
                       :password "password"
                       :host "localhost"
                       :port 3306})


(def multi-file-edn {:type :multi-file-edn
                       :directory "/home/marcus/data/event-store"})

(def all-event-names #{:vessel-created :vessel-updated :position-reported :period-added :fixture-created})
                                  
(defn prune-full-snapshot! [storage storage-projections vessels]
  (doall (for [s vessels]
    (do
      (def tm (b/benchmark
              (def aggregate (d/load-aggregate-es- storage :vessel s))
              (d/dispatch-es! storage :vessel s "aggregate.vessel/create-vessel-full-snapshot" aggregate)                
              (s/delete-events! storage s
                                (filter (fn [e]
                                          (some #(= (:n e) %) all-event-names)
                                          ) (s/stream- storage s true)))))
      {:a-id s :time-ms tm}
      ))))

(defn prune-superseeded! [storage storage-projections vessels]
  (doall (for [s vessels]
    (do
      (def tm (b/benchmark
      (def events (s/stream- storage s true))
      (def remove-events (concat
                          (butlast (filter #(= (:n %) :vessel-updated) events))
                          (butlast (filter #(= (:n %) :position-reported) events))
                          ))

      (s/delete-events! storage s remove-events)))
      {:a-id s :time-ms tm}
      ))))

(defn prune-bounded! [storage storage-projections keep-num-events vessels]
  (doall (for [s vessels]
           (do
             (def tm (b/benchmark
             (def all-events (s/stream- storage s true))
             (def keep-events (reverse (take keep-num-events (reverse all-events))))
             (def snapshot-events (take (- (count all-events) keep-num-events) all-events))
             (def aggregate (d/load-aggregate-es- storage :vessel s snapshot-events))

             (if (not (empty? aggregate)) (do
                                            (s/delete-stream! storage s true)
                                            (d/dispatch-es! storage :vessel s "aggregate.vessel/create-vessel-full-snapshot" aggregate))
                 (doseq [e keep-events]
                   (s/persist-event! storage (assoc e :s s) true)))))

             {:a-id s :time-ms tm}
             ))))

(defn prune-probabilistic! [storage storage-projections now vessels]
  (doall (for [s vessels]
           (do
             (def tm (b/benchmark
             (def events (s/stream- storage s true))
             (def remove-events (concat
                                 (butlast (filter #(= (:n %) :vessel-updated) events))
                                 (butlast (filter #(= (:n %) :position-reported) events))
                                 (filter #(and
                                           (= (:n %) :period-added)
                                           (not (or (>= (get-in % [:p :from]) now)
                                                    (>= (get-in % [:p :to]) now)))) events)))

             (doseq [e remove-events] (s/delete-event! storage s (assoc e :s s)))))
             {:a-id s :time-ms tm}))))

(defn remove-events-for-window [from to remove-every-nth now events]
  (take-nth remove-every-nth (let [day 86400000
        to-ms (- now (* day from)) ;periods switched
        from-ms (- now (* day to))] ;periods switched

    (for [e events :when (and
                          (= (:n e) :position-reported)
                          (>= (get-in e [:p :received]) from-ms)
                          (<= (get-in e [:p :received]) to-ms)
                          )]
      e)
    )))

(defn prune-hierarchical! [storage storage-projections now windows vessels]
  (doall (for [s vessels]
           (do
             (def tm (b/benchmark
                      (doseq [[from to remove-every-nth] (partition 3 windows)]
                        (do
                          (def events (s/stream- storage s true))
                          (s/delete-events! storage s (remove-events-for-window from to remove-every-nth now events))
                          ))))
             {:a-id s :time-ms tm}))))
