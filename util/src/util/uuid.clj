(ns util.uuid)
(defn create-uuid-string [] (.toString (java.util.UUID/randomUUID)))
(defn create-uuid [] (java.util.UUID/randomUUID))
(defn parse-uuid [uuid] (try
    (java.util.UUID/fromString uuid)
    (catch Exception e nil)))
(defn valid-uuid? [uuid] (uuid? (parse-uuid uuid)))
