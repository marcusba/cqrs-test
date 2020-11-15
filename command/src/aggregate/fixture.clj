(ns aggregate.fixture (:require
                   [clojure.tools.namespace.repl :refer [refresh]]
                   [command.logging :as log]
                   [command.apply]
                     ))
;===== all events -> pass inn events to apply them to the aggregate
(defmulti apply-fixture-event (fn [aggregate event] (:n event)))

(defmethod apply-fixture-event :fixture-created [aggregate {{:keys [id from to vessel-id]} :p :as event}]
  (log/apply-event :fixture-created aggregate event)
  (assoc aggregate :id id :from from :to to :vessel-id vessel-id)) 

;===== hydration trigger => start aggregate hydration
(defmethod command.apply/apply-events :fixture [a-type aggregate events]
  (reduce apply-fixture-event aggregate events))

;===== all commands => produces events
(defn create-fixture [aggregate params]
  (log/execute-command :fixture-crated aggregate params)
  (vector {:s (:id params) :n :fixture-created :p params}))
