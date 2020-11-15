(ns query.all-fixtures (:require
                      [event-store.storage :as s]
                      ))

(defmulti on-x! (fn [connection a-type event] (keyword (str (name a-type) "#" (name (:n event))))))
(defmethod on-x! :fixture#fixture-created [connection a-type event]

  (def existing-all-fixtures (s/projection-query- connection ["select * from all_fixtures where aggregate = ?" (get-in event [:p :id])]))

  (if (= (count existing-all-fixtures) 0)
    (s/projection-insert! connection :all_fixtures {:aggregate (get-in event [:p :id])}))


  )


(defn delete-all-data! [connection]    (clojure.java.jdbc/delete! (s/connection-map connection) :all_fixtures []))

(defn get-all-fixture-ids- [connection] (map #(:aggregate %) (s/projection-query- connection ["select * from all_fixtures"])))
