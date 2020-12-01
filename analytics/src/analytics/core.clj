(ns analytics.core (:require
                                [clojure.tools.namespace.repl :refer [refresh]]
                                [clojure.java.io :as io]
                                [clojure.tools.reader.edn :as edn]
                                [clojure.pprint]
                                [clojure.core.matrix :as m]
                                [clojure.core.matrix.stats :as s]
                                [util.math]
                                [analytics.stats]
                                [clojure.string]
                    ))

(def config {:source-directory "/mnt/data/testrunner"
             :target-directory "/mnt/data/analytics"})

(def pruning-algorithms [:no-pruning :superseeded :bounded :probabilistic :hierarchical :full-snapshot :command-sourcing])

(def event-stores [:mongodb :mysql :multi-file-edn])

(defn pprint-to-string [o]
  (def out (java.io.StringWriter.))
  (clojure.pprint/pprint o out)
  (.toString out))

(defn ensure-dirs! [directory] (io/make-parents (str directory "/no-file.txt")))

(defn read-directory-content- [directory]
  (ensure-dirs! directory)
  (for [f (-> directory io/file file-seq)
        :when (and
               (not (.isDirectory f))
               (= (.getParent f) directory))]
    f))


(defn read-tests- [{:keys [source-directory]}]
  (reduce #(conj %1 (edn/read-string (slurp %2 :encoding "utf-8")))
          [] (read-directory-content- source-directory)))

(defn extract-aggregates [event-store pruning tests] (reduce #(concat %1 (get-in %2 [event-store pruning :aggregates])) [] tests))

;extract data
(defn values-measurement [event-store tests pruning value]
  (sort (reduce #(conj %1 (util.math/round (value %2) 2)) [] (extract-aggregates event-store pruning tests))))

(defn matrix-measurement [event-store tests pruning value value2]
  (sort (map #(vector (util.math/round (value %) 2) (util.math/round (value2 %) 2)) (extract-aggregates event-store pruning tests))))

(defn stats-map-measurement [event-store tests pruning value]
  (analytics.stats/stats-map (values-measurement event-store tests pruning value)))

;aggregate data
(defn extract-all-stats [event-store tests value]
  (for [pruning pruning-algorithms]
    (conj (select-keys (stats-map-measurement event-store tests pruning value) [:min :max :mean :mode :median :size :total :sd-population-1 :sd-population-6]) [:pruning pruning])))

;diagram data
(defn print-scatter-plot- [event-store tests x y pruning]
  (doseq [a (extract-aggregates event-store pruning tests)] (println (x a) (util.math/round (y a) 2) (name pruning))))

(defn print-table-row- [pruning tests value statistic]
  (def all-stats (reduce #(concat %1 %2) []
                         (for [event-store event-stores] (extract-all-stats event-store tests value))))
  (def needed-stats (for [stat all-stats :when (= (:pruning stat) pruning)] (util.math/round (statistic stat) 2)))

  (println (clojure.string/capitalize (name pruning)) "&" (clojure.string/join " & " needed-stats) "\\\\")
  (println "\\toprule[0.0001em]")
    )

(defn print-table- [tests value statistic]
  (doseq [pruning pruning-algorithms]
    (print-table-row- pruning tests value statistic)))

