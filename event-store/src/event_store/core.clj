(ns event-store.core
  (:require
   [clojure.tools.namespace.repl :refer [refresh]]
   [event-store.storage :as storage]
   ))

(def mysql {:type :mysql
                       :name "event_store"
                       :user "event_store"
                       :password "password"
                       :host "localhost"
                       :port 3306})

(def multi-file-edn {:type :multi-file-edn
                       :directory "/home/marcus/data/event-store"})

(def mongodb {:type :mongodb
              :name "event-store"
              :user "event-store"
              :password "password"
              :host "localhost"
              :port 27017})

(def projection-connection mysql)
(def event-store-connection (atom mongodb))



