(ns command.logging)


(def do-log-apply? false)
(def do-log-command? false)
(def do-log-generated? false)

(defn apply-event [event-type aggregate event] (if do-log-apply? (println "-> Apply (event/aggregate/event)" event-type aggregate event)))
(defn execute-command [command aggregate params] (if do-log-command? (println "-> Command (command/aggregate/params)" command aggregate params)))
(defn generated-events [events] (if do-log-generated? (println "-> Generated events" events)))
