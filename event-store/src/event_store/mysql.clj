(ns event-store.mysql (:require 
                       [clojure.tools.namespace.repl :refer [refresh]]
                       [event-store.storage :as s]
                       [clojure.java.jdbc]
                       [clojure.pprint]
))

(defn table-name [e?] (if (or (true? e?) (= 1 e?)) :event :command))

(defmethod s/event-map :mysql [connection {:keys [s n p] :as event} e?]
  (def p-str (pr-str p))
  (def p-str-length (alength (.getBytes p-str "utf-8")))
  {:t (util.unixtime/timestamp)
   :s s
   :n (name n)
   :e (if (= e? true) 1 0)
   (if (> p-str-length 21000) :pb :p) p-str}) ; use pb (blob) if too long

(defmethod s/connection-map :mysql [{:keys [type name user password host port]}]
  {:dbtype "mysql"
   :dbname name
   :user user
   :password password
   :host host
   :port port})

(defmethod s/persist-event! :mysql [connection event e?]
  (clojure.java.jdbc/insert! (s/connection-map connection) (table-name e?) (s/event-map connection event e?)))

(defmethod s/delete-streams! :mysql [connection]
  (clojure.java.jdbc/delete! (s/connection-map connection) :event [])
  (clojure.java.jdbc/delete! (s/connection-map connection) :command []))

(defmethod s/delete-stream! :mysql [connection s e?]
  (if e?
  (clojure.java.jdbc/delete! (s/connection-map connection) :event ["s = ?" s])
  (clojure.java.jdbc/delete! (s/connection-map connection) :command ["s = ?" s])))

(defmethod s/stream- :mysql [connection s e?]
  (vec (map #(s/event-map-from-storage %)
   (clojure.java.jdbc/query (s/connection-map connection) [(str "select * from " (name (table-name e?)) " where s = ? and e = ? order by t asc") s (if (= e? true) 1 0)]))))

(defmethod s/delete-event! :mysql [connection s event]
  (clojure.java.jdbc/delete! (s/connection-map connection) :event ["s = ? and t = ?" s (:t event)]))

(defmethod s/delete-events! :mysql [connection s events]
  (doseq [event events]
    (s/delete-event! connection s event)))

(defmethod s/backup-event-store! :mysql [connection]
  (clojure.java.jdbc/delete! (s/connection-map connection) :event_backup [])
  (clojure.java.jdbc/execute! (s/connection-map connection) ["insert event_backup select * from event"]))

(defmethod s/restore-event-store! :mysql [connection]
  (clojure.java.jdbc/delete! (s/connection-map connection) :event [])
  (clojure.java.jdbc/execute! (s/connection-map connection) ["insert event select * from event_backup"]))


(defmethod s/projection-insert! :mysql [connection table m] (clojure.java.jdbc/insert! (s/connection-map connection) table m))
(defmethod s/projection-delete! :mysql [connection table q] (clojure.java.jdbc/delete! (s/connection-map connection) table q))
(defmethod s/projection-update! :mysql [connection table m q] (clojure.java.jdbc/update! (s/connection-map connection) table m q))
(defmethod s/projection-query- :mysql [connection q] (clojure.java.jdbc/query (s/connection-map connection) q))

