(ns aggregate.vessel (:require
                   [clojure.tools.namespace.repl :refer [refresh]]
                   [command.logging :as log]
                   [command.apply]
                   [util.unixtime]
                     ))

;============================ HYDRATE STATE ========================

;===== all events -> pass inn events to apply them to the aggregate
(defmulti apply-vessel-event (fn [aggregate event] (:n event)))

(defmethod apply-vessel-event :vessel-created [aggregate {{:keys [id name built]} :p :as event}]
  (log/apply-event :vessel-created aggregate event)
  (assoc aggregate :id id :name name :built built :periods [] :positions [] :last-position nil :last-updated nil))

(defmethod apply-vessel-event :period-added [aggregate {:keys [p] :as event}]
  (log/apply-event :period-added aggregate event)
  (update aggregate :periods conj p))

(defmethod apply-vessel-event :position-reported [aggregate {:keys [p] :as event}]
  (log/apply-event :position-reported aggregate event)
  (-> aggregate
      (#(update % :positions conj p))
      (#(assoc % :last-position p))
      ))

(defmethod apply-vessel-event :vessel-updated [aggregate {:keys [p] :as event}]
  (log/apply-event :vessel-updated aggregate event)
  (assoc aggregate :last-updated (:when p)))


;pruning event handlers
(defmethod apply-vessel-event :vessel-created-full-snapshot [aggregate {:keys [p] :as event}]
  (log/apply-event :vessel-created-full-snapshot aggregate event)
  p)

;===== hydration trigger => start aggregate hydration
(defmethod command.apply/apply-events :vessel [a-type aggregate events]
  (reduce apply-vessel-event aggregate events))


;================================ EXECUTE COMMANDS ===================

;===== all commands => produces events
(defn create-vessel [aggregate params]
  (log/execute-command :create-vessel aggregate params)
  (vector
   {:s (:id params) :n :vessel-created :p params}
   {:s (:id params) :n :vessel-updated :p { :when (util.unixtime/timestamp)}}   
   ))

(defn add-period [aggregate params]
  (log/execute-command :add-period aggregate params)
  (vector
   {:s (:id aggregate) :n :period-added :p params}
   {:s (:id aggregate) :n :vessel-updated :p { :when (util.unixtime/timestamp)}}   
  ))

(defn report-position [aggregate params]
  (log/execute-command :report-position aggregate params)
  (vector
   {:s (:id aggregate) :n :position-reported :p params}
   ;{:s (:id aggregate) :n :vessel-updated :p { :when (util.unixtime/timestamp)}}   
   ))

;===== Pruning commands ======
(defn create-vessel-full-snapshot [aggregate params]
  (log/execute-command :create-vessel-full-snapshot aggregate params)
  (vector
   {:s (:id params) :n :vessel-created-full-snapshot :p params}
   ))

