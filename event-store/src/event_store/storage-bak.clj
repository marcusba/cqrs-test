(ns event-store.storage (:require 
                         [clojure.tools.namespace.repl :refer [refresh]]
                         [clojure.tools.reader.edn]
   ))


(defn event-map [{:keys [s n p]} e?] {:t (util.unixtime/timestamp)
                                                                :s s
                                                                :n n
                                                                :e (if (= e? true) 1 0)
                                                                :p (pr-str p)})

(defn event-map-from-storage [{:keys [t s n p]}] {:t t
                                                :s s
                                                :n n
                                                :p (clojure.tools.reader.edn/read-string p)})

(defn mysql-connection-map [{:keys [type name user password host port]}] {:dbtype "mysql"
                                                                :dbname name
                                                                :user user
                                                                :password password
                                                                :host host
                                                                :port port})

(defn persist-event! [connection event e?] (clojure.java.jdbc/insert! (mysql-connection-map connection) "event" (event-map event e?)))

(defn delete-streams! [connection] (clojure.java.jdbc/delete! (mysql-connection-map connection) "event" []))

(defn stream [connection s e?]
  (map #(event-map-from-storage %)
  (clojure.java.jdbc/query (mysql-connection-map connection) ["select * from event where s = ? and e = ? order by t asc" s (if (= e? true) 1 0)])))
