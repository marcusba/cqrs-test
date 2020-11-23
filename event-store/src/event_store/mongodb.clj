(ns event-store.mongodb (:require 
                       [clojure.tools.namespace.repl :refer [refresh]]
                       [event-store.storage :as s]
                       [clojure.java.jdbc]
                       [clojure.pprint]
                       [monger.core :as mg]
                       [monger.credentials :as mcr]
                       [monger.collection :as mc]
                       [util.uuid :as uu]
                       ))

;(mg/set-default-write-concern! 0)

(def existing-connection (atom nil))

(defn table-name [e?] (if (or (true? e?) (= 1 e?)) :event :command))

(defmethod s/event-map :mongodb [connection {:keys [s n p] :as event} e?]
  {:t (util.unixtime/timestamp)
   :n (clojure.string/replace (str n) #":" "")
   :p p})

;oh.. not a map
(defmethod s/connection-map :mongodb [{:keys [type name user password host port]}]
  (str host ":" port))
  
(defn db-map [{:keys [type name user password]}]
  [user
   name
   (.toCharArray password)])

(defn connect [connection]
  (if (nil? @existing-connection)
    (reset! existing-connection (mg/get-db (mg/connect-with-credentials (s/connection-map connection) (apply mcr/create (db-map connection))) (:name connection)))
    @existing-connection))

(defmethod s/persist-event! :mongodb [connection event e?]
  (def existing-stream (s/stream- connection (:s event) e?))

  (def data {:s (:s event)
              :entities (conj
                         (if (nil? existing-stream) [] (vec (map #(assoc % :n (clojure.string/replace (str (:n %)) #":" "")) existing-stream)))
                         (s/event-map connection event e?))})

  (mc/update (connect connection) (if e? "event" "command")
             {:_id (uu/parse-uuid (:s event))}
             data
             {:upsert true}))

(defmethod s/delete-streams! :mongodb [connection]
  (mc/remove (connect connection) "event")
  (mc/remove (connect connection) "command"))
  
(defmethod s/delete-stream! :mongodb [connection s e?]
  (mc/remove (connect connection) (if e? "event" "command") {:_id (uu/parse-uuid s)}))

(defmethod s/stream- :mongodb [connection s e?]
  (if (empty? s)
    []
    (map #(assoc % :n (keyword (:n %))) (:entities (mc/find-map-by-id (connect connection) (if e? "event" "command") (uu/parse-uuid s))))))

(defmethod s/delete-event! :mongodb [connection s event]
  (def stream (first (mc/find-maps (connect connection) "event" {:_id (uu/parse-uuid s)})))
  (def modified-events (filter
                        #(not (and
                          (= (:t %) (:t event))
                          (= (:p %) (:p event))))
                        (:entities stream)))

  (if (empty? modified-events)
    (s/delete-stream! connection s true) ; no events left.. remove.
    (mc/update (connect connection) "event"
               {:_id (uu/parse-uuid s)}
               (assoc stream :entities modified-events)
             {:upsert false})))

(defmethod s/delete-events! :mongodb [connection s events]
  (doseq [event events]
    (s/delete-event! connection s event)))

(defmethod s/backup-event-store! :mongodb [connection]
  (mc/remove (connect connection) "event_backup")
  (mc/insert-batch (connect connection) "event_backup"
                   (mc/find-maps (connect connection) "event")))

(defmethod s/restore-event-store! :mongodb [connection]
  (mc/remove (connect connection) "event")
  (mc/insert-batch (connect connection) "event"
                   (mc/find-maps (connect connection) "event_backup")))

