(ns event-store.storage (:require 
                         [clojure.tools.namespace.repl :refer [refresh]]
                         [clojure.tools.reader.edn]
                         [util.unixtime]
                         [clojure.pprint]
   ))

(defn event-map-from-storage [{:keys [t s n p pb]}] {:t t
                                                  :n (keyword n)
                                                  :p (clojure.tools.reader.edn/read-string (if (nil? pb) p pb))})

(defmulti connection-map (fn [connection] (:type connection)))
(defmulti persist-event! (fn [connection event e?] (:type connection)))
(defmulti delete-streams! (fn [connection] (:type connection)))
(defmulti delete-stream! (fn [connection s e?] (:type connection)))
(defmulti stream- (fn [connection s e?] (:type connection)))
(defmulti event-map (fn [connection e e?] (:type connection)))

(defmulti delete-event! (fn [connection s event] (:type connection)))
(defmulti delete-events! (fn [connection s events] (:type connection)))

(defmulti backup-event-store! (fn [connection] (:type connection)))
(defmulti restore-event-store! (fn [connection] (:type connection)))

(defmulti projection-insert! (fn [connection table m] (:type connection)))
(defmulti projection-delete! (fn [connection table q] (:type connection)))
(defmulti projection-update! (fn [connection table m q] (:type connection)))
(defmulti projection-query- (fn [connection q] (:type connection))) 

                                        ;one require for each storage implementation
(require 'event-store.mysql)
(require 'event-store.multi-file-edn)
(require 'event-store.mongodb)


