(ns util.math (:require
                   [clojure.tools.namespace.repl :refer [refresh]]
                   )
    )

(defn round "Round a double to the given precision (number of significant digits)"
  [d precision]
  (let [factor (Math/pow 10 precision)]
    (/ (Math/round (* d factor)) factor)))
