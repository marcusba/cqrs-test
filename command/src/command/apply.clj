(ns command.apply)

(defmulti apply-events "Build state" (fn [a-type aggregate events] a-type))
