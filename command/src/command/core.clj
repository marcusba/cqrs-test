(ns command.core
  (:require
   [clojure.tools.namespace.repl :refer [refresh]]
   [event-store.storage :as storage]
   [command.dispatch :as d]
   [aggregate.vessel]
   [util.benchmarking :as b]

   ))

(def mysql {:type :mysql
                       :name "event_store"
                       :user "event_store"
                       :password "password"
                       :host "localhost"
                       :port 3306})

(def multi-file-edn {:type :multi-file-edn
                       :directory "/home/marcus/data/event-store"})

(def projection-connection mysql)
(def event-store-connection (atom multi-file-edn))



