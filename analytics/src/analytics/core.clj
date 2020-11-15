(ns analytics.core (:require
                                [clojure.tools.namespace.repl :refer [refresh]]
                                [clojure.java.io :as io]
                                [clojure.tools.reader.edn :as edn]
                                [clojure.pprint]
                                [clojure.core.matrix :as m]
                                [clojure.core.matrix.stats :as s]
                    ))

(def config {:source-directory "/home/marcus/data/testrunner"
             :target-directory "/home/marcus/data/analytics"})

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
