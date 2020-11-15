(ns command.dispatch (:require
                      [clojure.tools.namespace.repl :refer [refresh]]
                      [event-store.storage :as s]
                      [clojure.pprint]
                      [util.uuid :as u]
                      [util.benchmarking :as b]
                      [util.unixtime :as ut]
                      [aggregate.vessel]
                      [aggregate.fixture]
                      [command.apply]
                      [command.logging :as log]
                      [query.event]
                      ))

(def mysql {:type :mysql
                       :name "event_store"
                       :user "event_store"
                       :password "password"
                       :host "localhost"
                 :port 3306})

(def multi-file-edn {:type :multi-file-edn
                     :directory "/home/marcus/data/event-store"})

(def connection mysql)

(defn load-aggregate-es- "Load aggregate for event sourcing. Pass in events as last parameter or fetch all from the store"
  ([connection a-type a-id] (command.apply/apply-events a-type {} (s/stream- connection a-id true)))
  ([connection a-type a-id events] (command.apply/apply-events a-type {} events)))

(defn load-aggregate-cs- "Load aggregate for command sourcing" [connection a-type a-id]
  (def all-commands (conj (s/stream- connection a-id false)))

  (loop [commands all-commands aggregate {}]
    (if (empty? commands)
      aggregate
      (recur (rest commands) (let [f (:n (first commands)) params (:p (first commands))]
                               (-> f
                                   symbol
                                   resolve
                                   (#(apply % [aggregate params])) ;call function and generate events
                                   (#(command.apply/apply-events a-type aggregate %)) ;appy events
                                   ))))))

(defn dispatch-es! "Execute command event sourced" [connection a-type a-id f params]
  (def events (-> f
                  symbol
                  resolve
                  (#(apply % [(load-aggregate-es- connection a-type a-id) params]))))

  (s/persist-event! connection {:s (if (empty? a-id) (:id params) a-id) :n f :p params} false) ; persist command.. not event
  (doall (map #(s/persist-event! connection % true) events))
  (doall (map #(query.event/emit! mysql a-type %) events))
  )

(defn dispatch-cs! "Execute command command sourced" [connection a-type a-id f params]
  (def aggregate (load-aggregate-cs- connection a-type a-id))

  (def events (-> f
      symbol
      resolve
      (#(apply % [aggregate params]))))

  (command.apply/apply-events a-type aggregate events)
  (s/persist-event! connection {:s (if (empty? a-id) (:id params) a-id) :n f :p params} false)
  (doall (map #(query.event/emit! mysql a-type %) events))
 )


(defn print-vessel [vessel positions? periods? fixture-periods-only?]
  (println "Name:" (:name vessel))
  (println "Built:" (ut/str-date (:built vessel)))
  (println "Last updated:" (ut/str-date-time (:last-updated vessel)))

  (println "Positions (lat/lng) (" (count (:positions vessel)) ")")

  (if positions?
  (doseq [p (:positions vessel)]
    (println (ut/str-date-time (:received p)) (:lat p) (:lng p))))

  (println "Periods (" (count (:periods vessel)) ")")

  (if periods?
    (doseq [p (:periods vessel)]
      (if (and fixture-periods-only? (not (= :fixture (:type p)))) nil
    (println (ut/str-date (:from p)) (ut/str-date (:to p)) (Math/round (double (/ (- (:to p) (:from p)) 86400000))) "d" (:type p) (:notes p))))))
