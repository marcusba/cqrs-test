(ns query.all-vessels (:require
                      [event-store.storage :as s]
                      ))

(defmulti on-x! (fn [connection a-type event] (keyword (str (name a-type) "#" (name (:n event))))))
(defmethod on-x! :vessel#vessel-created [connection a-type event]

  (def existing-all-vessels (s/projection-query- connection ["select * from all_vessels where aggregate = ?" (get-in event [:p :id])]))
  (if (= (count existing-all-vessels) 0)
    (s/projection-insert! connection :all_vessels {:aggregate (get-in event [:p :id])}))

)

(defn delete-all-data! [connection]    (clojure.java.jdbc/delete! (s/connection-map connection) :all_vessels []))


(defn get-all-vessel-ids- [connection] (map #(:aggregate %) (s/projection-query- connection ["select * from all_vessels"])))

