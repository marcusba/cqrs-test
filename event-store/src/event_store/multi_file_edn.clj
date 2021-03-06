(ns event-store.multi-file-edn (:require 
                                [clojure.tools.namespace.repl :refer [refresh]]
                                [event-store.storage :as s]
                                [clojure.java.io :as io]
                                [clojure.tools.reader.edn :as edn]
                                ))

(defn ensure-dirs! [directory] (io/make-parents (str directory "/backup/no-file.txt")))

(defn stream-file [{:keys [directory]} s e?]
  (str directory "/" s "-" (if e? "e" "c")))

(defn stream-exists? [connection s e?]
  (.exists (io/file (stream-file connection s e?))))

(defn read-directory-content- [directory]
  (ensure-dirs! directory)
  (for [f (-> directory io/file file-seq)
        :when (and
               (not (.isDirectory f))
               (= (.getParent f) directory))]
    f))

(defn empty-directory! [directory]
  (doall (map #(io/delete-file %) (read-directory-content- directory))))

(defn copy-directory! [source-directory destination-directory]
  (doall (map #(io/copy % (io/file (str destination-directory "/" (.getName %)))) (read-directory-content- source-directory))))

(defn persist-events! [{:keys [directory] :as connection} events e?]
  (ensure-dirs! directory)
  (doseq [event events]
  (spit (stream-file connection (:s event) e?) (s/event-map connection event e?) :append true :encoding "utf-8")))

(defmethod s/event-map :multi-file-edn [connection {:keys [s n p]} e?]
  {:t (util.unixtime/timestamp)
   :n (name n)
   :p (pr-str p)})

;not needed
;(defmethod s/connection-map :multi-file-edn [{:keys [directory]}] {:directory directory})

(defmethod s/persist-event! :multi-file-edn [{:keys [directory] :as connection} event e?]
  (ensure-dirs! directory)
  (spit (stream-file connection (:s event) e?) (s/event-map connection event e?) :append true :encoding "utf-8"))

(defmethod s/delete-streams! :multi-file-edn [{:keys [directory] :as storage}]
  (ensure-dirs! directory)
  (empty-directory! directory))

(defmethod s/delete-stream! :multi-file-edn [{:keys [directory] :as storage} s e?]
  (ensure-dirs! directory)
  (io/delete-file (stream-file storage s true))
)

(defmethod s/stream- :multi-file-edn [connection s e?]
  (if (stream-exists? connection s e?)
   (vec (map #(s/event-map-from-storage %)
    (edn/read-string (str "[" (slurp (stream-file connection s e?) :encoding "utf-8") "]"))))
   []))

(defmethod s/delete-event! :multi-file-edn [{:keys [directory] :as connection} s event]
  (def events (filter #(not (= (dissoc event :s) %)) (s/stream- connection s true)))
  (s/delete-stream! connection s true) 
  (doseq [e events]
    (s/persist-event! connection (assoc e :s s) true)))

(defmethod s/delete-events! :multi-file-edn [{:keys [directory] :as connection} s remove-events]
  (def events (filter
               (fn [e] (not (some #(= (dissoc e :s) %) remove-events)))
               (s/stream- connection s true)))
  
  (s/delete-stream! connection s true)
  (persist-events! connection (map #(assoc % :s s) events) true)

  )

(defmethod s/backup-event-store! :multi-file-edn [{:keys [directory]}]
  (ensure-dirs! directory)
  (empty-directory! (str directory "/backup"))
  (copy-directory! directory (str directory "/backup")))
  
(defmethod s/restore-event-store! :multi-file-edn [{:keys [directory]}]
  (ensure-dirs! directory)
  (empty-directory! directory)
  (copy-directory! (str directory "/backup") directory))

