(ns query.event (:require
                 [clojure.tools.namespace.repl :refer [refresh]]
                 [query.all-vessels] ;projections
                 [query.all-fixtures] 
  ))

(defmulti emit! "Emit events to projection event handlers" (fn [connection a-type event] (keyword (str (name a-type) "#" (name (:n event))))))
(defmethod emit! :vessel#vessel-created [connection a-type event]  
  (query.all-vessels/on-x! connection a-type event)

  )

(defmethod emit! :vessel#period-added [connection a-type event]  
; no interest in this event
  )
(defmethod emit! :vessel#position-reported [connection a-type event]  
; no interest in this event
  )
(defmethod emit! :vessel#vessel-updated [connection a-type event]  
; no interest in this event
  )


;pruning events..
(defmethod emit! :vessel#vessel-created-full-snapshot [connection a-type event]  
; no interest in this event
  )





(defmethod emit! :fixture#fixture-created [connection a-type event]
  
  (query.all-fixtures/on-x! connection a-type event)

  )

