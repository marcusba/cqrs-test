(ns testrunner.core (:gen-class) (:require
                     [clojure.tools.namespace.repl :refer [refresh]]
                     [clojure.pprint]
                     [clojure.edn]
                     [util.uuid]
                     [clojure.java.jdbc]
                     [util.unixtime :as ut]
                     [util.benchmarking :as b]
                     [event-store.storage :as s]
                     [command.dispatch :as d]
                     [pruning.core :as p]
                     [query.all-vessels :as qv]
                     [query.all-fixtures]
                     [java-time :as t]
                     [clj-memory-meter.core :as mm]
                     [clojure.java.io :as io]
                     [clojure.core.matrix.stats :as stats]
                     [clojure.java.shell :refer [sh]]
  ))

(defn rand-range-int [from to] "Rand within interval. Inc from exc to" (+ (rand-int (- to from)) from))
(defn rand-range [from to] "Rand within interval. Inc from exc to" (+ (rand (- to from)) from))

(defn assign-randomly-to-seq [value min-value-per-assignment max-value-per-assignment]
  (loop [res []]
    (if (>= (reduce + res) value)
      res
      (recur (conj res (rand-range-int min-value-per-assignment (inc max-value-per-assignment)))
             ))))
   
(defn rand-range-int-seq [num-elements min max]
  (map (fn [_] (rand-range-int min (inc max))) (range num-elements)))

(defn percentile-data [pst v] "pst = Target %. v = Vector with alternating values of percentile / data. Returns data of target percentile."
  (last (first
         (filter
          #(<= pst (first %))
          (partition 2 v)))))

(defn randomize-percentile [v coll] "v = Vector with alternating values of percentile / data. Returns data of target percentile. coll = target data."
  (shuffle (map-indexed (fn [i e]
                 (percentile-data (* (/ (inc i) (count coll)) 100) v))
                 (range (count coll)))))

(defn find-group [num v] "num = Target. v = vector of ints. Returns element num where num resides. E.g. num = 31, v = [10 10 10]. Returns 3"
  (def all-groups (vec (map-indexed #(vector % %2) v)))
  (loop [groups all-groups accumulated 0]
    (let [current-group (first groups)
          [idx val] current-group
          accumulated-current (+ accumulated (or val 0))]
    (if (or (nil? current-group) (>= accumulated-current num))
      idx
      (recur (rest groups) accumulated-current)
      ))))

(defn select-by-weight [entities pst weight] "Select random entity based on weight. Weight and pst must be > 0"
  (def weights (map-indexed (fn [idx e]
                        (def p (* (/ (inc idx) (count entities)) 100))
                        (if (<= p pst) weight 1))
                                    entities))

  (def max-r (reduce + weights))
  (def selected-val (rand max-r))

  (def selected-idx (loop [rest-weights weights
                           idx 0
                           total (first rest-weights)]
                      (if (<= selected-val total)
                        idx
                        (recur (rest rest-weights) (inc idx) (+ total (first rest-weights))))))

  (get (vec entities) selected-idx))

; end helper

;start config
; population vessels ))
(def num-vessels (rand-range-int 12 31))
(def test-config
  {
   ;simulation
   :num-vessels num-vessels ; num vessels (population = 120 - 313 with an average age of 6-9 years) to be generated see \cite{obtainingContracts} p 19
   :min-vessel-age 0 ; see \cite{obtainingContracts} p 19
   :max-vessel-age 11 ; see \cite{obtainingContracts} p 19
   :preferred-vessels [40 1.3] ; 40% of vessels are preffered with weight at 1.3. Not from any source.
   :min-vessel-utilization 0.31 ; \cite{obtainingContracts} p 20
   :max-vessel-utilization 0.72 ; \cite{obtainingContracts} p 20
   :min-contract-length 3 ; min days of contract. spot market is <= 30 days
   :max-contract-length 30 ; max days of contract. spot market is <= 30 days
   :ais-event-resolution 24 ; hours between each report.. track candidate vessels for a given area

   ;dates
   :start-date-time (t/zoned-date-time 2021 1 1) ; simulation starts
   :end-date-time (t/zoned-date-time 2031 12 31) ; simulation ends

   ;pruning
   :bounded-buffer 100 ; keep last 50 events. snapshot of the rest
   :hierarchical-windows [30 59 2 60 99999 4] ; days 30 - 60 keep every 2. After that keep every 4

   ;event-storage
   :storage-mysql {:type :mysql
                       :name "event_store"
                       :user "event_store"
                       :password "password"
                       :host "localhost"
                       :port 3306}
   :storage-multi-file-edn {:type :multi-file-edn
                            :directory "/mnt/data/event-store"}
   :storage-mongodb {:type :mongodb
              :name "event-store"
              :user "event-store"
              :password "password"
              :host "localhost"
              :port 27017}

   ;projection storage - same for all
   :projections-storage {:type :mysql
                       :name "event_store"
                       :user "event_store"
                       :password "password"
                       :host "localhost"
                         :port 3306}

   ;test orchestration
   :random-data-sets 1 ;times the whole suite of tests will be run. i.e. how many distinct data sets we will generate
   :test-iterations 1000 ;how many times each test will run for each pruning method e.g. loading an aggregate 1000 times
   :cooldown-time 120 ;sec delay before each test set (per pruning)
   :test-output-dir "/mnt/data/testrunner"
   })

(defn generate-sensitivity-test-ais-resolution-config [ais-event-resolution]
  (merge test-config {
   :num-vessels 1
   :min-vessel-age 6
   :max-vessel-age 6
   :min-vessel-utilization 0.50
   :max-vessel-utilization 0.50
   :ais-event-resolution ais-event-resolution
}))

(defn generate-sensitivity-test-utilization-config [utilization]
  (merge test-config {
   :num-vessels 1
   :min-vessel-age 6
   :max-vessel-age 6
   :min-vessel-utilization utilization
   :max-vessel-utilization utilization
}))

; command generation functions:

(defn generate-period-command [a-id type from to notes]
         {:a-type :vessel
          :a-id a-id
          :command "aggregate.vessel/add-period"
          :p {:id (util.uuid/create-uuid-string)
              :from from
              :to to
              :type type
              :notes notes}}
  )

(defn generate-ais-report-command [a-id type received]
         {:a-type :vessel
          :a-id a-id
          :command "aggregate.vessel/report-position"
          :p {:id (util.uuid/create-uuid-string)
              :received received
              :type type
              :lat (rand-range-int 60 53)
              :lng (rand-range-int -4 8)
              }
          }
  )

(defn generate-fixture-command [from to]
    {:a-type :fixture
          :a-id ""
          :command "aggregate.fixture/create-fixture"
          :p {:id (util.uuid/create-uuid-string)
              :from (ut/from-zoned-date-time from)
              :to (ut/from-zoned-date-time to)
              }})

(defn generate-idle-commands [fixture prev-fixture vessel] []) ; enable below to add idle periods
(defn old-generate-idle-commands [fixture prev-fixture vessel]

  (def from (let []
              (def tmp-from (if (nil? prev-fixture)
                (get-in vessel [:p :built])
                ( + 1 (get-in prev-fixture [:p :to]))))))

  (def to (let []
              (def tmp-to (- (get-in fixture [:p :from]) 1))))

  (def days-14 (* 86400000 14))

  (if (= from to)
    []
     (if (> (- to from) days-14) ;split in tow if over 14 days
       [(generate-period-command (get-in vessel [:p :id]) :vessel from (- (+ from days-14) 1) "Some first idle period")
        (generate-period-command (get-in vessel [:p :id]) :vessel (+ from days-14) to "Some other idle period")]
       
       (if (< (- to from) 0) []
           [(generate-period-command (get-in vessel [:p :id]) :vessel from to "Some idle period")])
       )))

(defn assign-fixture-to-vessel [fixture vessels vessel-fixtures preferred-vessels]
  (loop [drawn-vessel (select-by-weight vessels (first preferred-vessels) (last preferred-vessels)) tries 0]
    (def drawn-vessel-id (get-in drawn-vessel [:p :id]))
    (def drawn-vessel-built (get-in drawn-vessel [:p :built]))
    (def fixtures-to-check (filter #(= drawn-vessel-id (get-in % [:p :vessel-id])) vessel-fixtures))
    (def overlapping-fixtures []) ;switch to enable overlap test on fixture periods
    (def old-overlapping-fixtures (filter (fn [f]
                                        (let [fixture-from (get-in fixture [:p :from])
                                              fixture-to (get-in fixture [:p :to])
                                              check-from (get-in f [:p :from])
                                              check-to (get-in f [:p :to])]

                                          (if (or
                                               (and (>= fixture-from check-from) (<= fixture-from check-to))
                                               (and (>= fixture-to check-from) (<= fixture-to check-to))
                                               ) true false)
                                          )) fixtures-to-check))
    (def prev-fixture (last fixtures-to-check))
    (def all-commands (conj (generate-idle-commands fixture prev-fixture drawn-vessel)
                            (generate-period-command drawn-vessel-id :fixture (get-in fixture [:p :from]) (get-in fixture [:p :to]) "")
                            (assoc-in fixture [:p :vessel-id] drawn-vessel-id)))
    (if (= tries (* (count vessels) 100)) (throw (Exception. "Could not assign all fixtures. No available vessel. Try again.")))
      

    (if (and (empty? overlapping-fixtures) (>= (get-in fixture [:p :from]) drawn-vessel-built)) ;update fixture command with ref to vessel.. ok if available + if old enough.. if not try again..
      (into vessel-fixtures all-commands)
      (recur (select-by-weight vessels (first preferred-vessels) (last preferred-vessels)) (inc tries))
      )))


(defn assign-fixtures-to-vessels [fixtures vessels preferred-vessels]
  (loop [unassigned-fixtures fixtures         
         vessel-fixtures []]
    (if (= (mod (count unassigned-fixtures) 1000) 0) (println (count unassigned-fixtures) "fixtures left to assign to vessels"))
    (if (empty? unassigned-fixtures)
      vessel-fixtures
      (recur (rest unassigned-fixtures) (assign-fixture-to-vessel (first unassigned-fixtures) vessels vessel-fixtures preferred-vessels))
    )))

(defn generate-vessel-commands [num-vessels min-vessel-age max-vessel-age end-date-time]
  (def ages (rand-range-int-seq num-vessels (* 12 min-vessel-age) (* 12 max-vessel-age)))
    
  (map (fn [vessel]
         {:a-type :vessel
          :a-id ""
          :command "aggregate.vessel/create-vessel"
          :p {:id (util.uuid/create-uuid-string)
              :name (str "Some vessel " vessel)
              :built (ut/from-zoned-date-time (t/adjust end-date-time t/minus (t/months (nth ages vessel))))}
           })
       (range num-vessels))
  )

(defn generate-fixture-commands [vessels min-vessel-utilization max-vessel-utilization min-contract-length max-contract-length start-date-time end-date-time]
  (def all-periods (ut/split-zoned-date-time-into-months start-date-time end-date-time))
  (loop [periods all-periods fixtures []]
    (def period (first periods))
    (def utilization (rand-range min-vessel-utilization (+ max-vessel-utilization 0.01)))
    (def available-vessels-for-this-period (count (filter #(<= (get-in % [:p :built]) (ut/from-zoned-date-time period)) vessels)))
    (def fixture-lengths (assign-randomly-to-seq (* available-vessels-for-this-period 30 utilization) min-contract-length max-contract-length))
    (if (nil? period)
      fixtures
      (recur (rest periods) (concat fixtures
                                    (doall (map (fn [l]
                                           (def from-day (rand-range-int 0 30)) ;0 - 29 plus first day so 1 - 30
                                           (def from (t/adjust period t/plus (t/days from-day)))
                                           (def to (ut/to-zoned-date-time (-
                                                                           (ut/from-zoned-date-time (t/adjust from t/plus (t/days l)))
                                                                           1
                                                                           ) "Europe/Oslo"))
                                           (generate-fixture-command from to))
                                         fixture-lengths)
                                    )))
      )))

(defn generate-ais-report-commands [vessels ais-event-resolution start-date-time end-date-time]
  (def start (ut/from-zoned-date-time start-date-time))
  (def end (ut/from-zoned-date-time end-date-time))
  (def time-delay (* ais-event-resolution 60 60 1000))

  (loop [cur-time start reports []]

    (def new-reports (filter #(not (nil? %))
                             (for [vessel vessels]
                               (if (>= cur-time (get-in vessel [:p :built]))
                                 (generate-ais-report-command (get-in vessel [:p :id]) :position cur-time) 
                                 nil
                                 )
                         )))
    
    (if (> cur-time end)
      reports
      (recur (+ cur-time time-delay) (into reports new-reports))
    ))
  )

(defn generate-commands [{:keys [num-vessels min-vessel-age max-vessel-age preferred-vessels min-vessel-utilization max-vessel-utilization min-contract-length max-contract-length ais-event-resolution start-date-time end-date-time]}]
  
  (def vessels (generate-vessel-commands num-vessels min-vessel-age max-vessel-age end-date-time))
  (println (count vessels) "vessels generated")
  (def fixtures (generate-fixture-commands vessels min-vessel-utilization max-vessel-utilization min-contract-length max-contract-length start-date-time end-date-time))
  (println (count fixtures) "fixtures generated")  
  (def assigned-fixtures (assign-fixtures-to-vessels fixtures vessels preferred-vessels))
  (println (count fixtures) "fixtures assigned")
  (println (- (count assigned-fixtures) (count fixtures)) "vessel periods generated")  
  (def ais-reports (generate-ais-report-commands vessels ais-event-resolution start-date-time end-date-time))
  (println (count ais-reports) "AIS reports generated")
  
  (def tot-commands (vec (concat
        vessels
        assigned-fixtures ; and vessel periods
        ais-reports
        )))

  (println (count tot-commands) "tot commands to be executed")
  tot-commands)

(defn reset-projections-store! [storage]
  (qv/delete-all-data! storage)
  (query.all-fixtures/delete-all-data! storage)
)

(defn reset-event-store! [storage]
  (s/delete-streams! storage))

(defn run-single-tests- [storage event-sourced? iterations aggregates pruning-data]
  (def results (doall (if event-sourced? 
    (map (fn [a] (doall (repeatedly iterations #(b/benchmark (d/load-aggregate-es- storage :vessel a))))) aggregates)
    (map (fn [a] (doall (repeatedly iterations #(b/benchmark (d/load-aggregate-cs- storage :vessel a))))) aggregates)
    )))

  (map-indexed (fn [idx e]
              {:a-id (nth aggregates idx)
                        :load-times-ms e
               :pruning-time-ms (:time-ms (nth pruning-data idx))}) results))

(defn cooldown [s] (println "Cooldown" s "s") (Thread/sleep (* 1000 s)))

(defn run-no-pruning-tests! [config storage]
  (s/restore-event-store! storage)
  (cooldown (:cooldown-time config))
  (println "Running no pruning tests" (:type storage))
  (run-single-tests- storage true (:test-iterations config) (qv/get-all-vessel-ids- (:projections-storage config)) nil)
)

(defn run-command-sourcing-tests! [config storage]
  (s/restore-event-store! storage)
  (cooldown (:cooldown-time config))
  (println "Running command sourcing tests" (:type storage))
  (run-single-tests- storage false (:test-iterations config) (qv/get-all-vessel-ids- (:projections-storage config)) nil)
)

(defn run-full-snapshot-tests! [config storage]
  (s/restore-event-store! storage)
  (def vessels (qv/get-all-vessel-ids- (:projections-storage config)))
  (println "Pruning for full snapshot tests")
  (def pruning-data (p/prune-full-snapshot! storage (:projections-storage config) vessels))
  (cooldown (:cooldown-time config))  
  (println "Running full snapshot tests" (:type storage))  
  (run-single-tests- storage true (:test-iterations config) vessels pruning-data)
  )

(defn run-superseeded-tests! [config storage]
  (s/restore-event-store! storage)
  (def vessels (qv/get-all-vessel-ids- (:projections-storage config)))
  (println "Pruning for superseeded")
  (def pruning-data (p/prune-superseeded! storage (:projections-storage config) vessels))
  (cooldown (:cooldown-time config)) 
  (println "Running superseeded tests" (:type storage))  
  (run-single-tests- storage true (:test-iterations config) vessels pruning-data)
  )

(defn run-bounded-tests! [config storage]
  (s/restore-event-store! storage)
  (def vessels (qv/get-all-vessel-ids- (:projections-storage config)))
  (println "Pruning for bounded")
  (def pruning-data (p/prune-bounded! storage (:projections-storage config) (:bounded-buffer config) vessels))
  (cooldown (:cooldown-time config)) 
  (println "Running bounded tests" (:type storage))  
  (run-single-tests- storage true (:test-iterations config) (qv/get-all-vessel-ids- (:projections-storage config)) pruning-data)
)

(defn run-probabilistic-tests! [config storage]
  (s/restore-event-store! storage)
  (def vessels (qv/get-all-vessel-ids- (:projections-storage config)))
  (println "Pruning for probabilistic tests")
  (def pruning-data (p/prune-probabilistic! storage (:projections-storage config) (ut/from-zoned-date-time (:end-date-time config)) vessels))
  (cooldown (:cooldown-time config)) 
  (println "Running probabilistic tests" (:type storage))  
  (run-single-tests- storage true (:test-iterations config) vessels pruning-data)
  )

(defn run-hierarchical-tests! [config storage]
  (s/restore-event-store! storage)
  (def vessels (qv/get-all-vessel-ids- (:projections-storage config)))
  (println "Pruning for hierarchical tests")
  (def pruning-data (p/prune-hierarchical! storage (:projections-storage config) (ut/from-zoned-date-time (:end-date-time config)) (:hierarchical-windows config) vessels))
  (cooldown (:cooldown-time config)) 
  (println "Running hierarchical tests" (:type storage))  
  (run-single-tests- storage true (:test-iterations config) vessels pruning-data)
  )

;metadata per aggregate
(defn add-aggregate-metadata- [storage test-results e?]

  (loop [tests test-results with-metadata []]
    (def t (first tests))
    (if (nil? t)
      with-metadata
      (do
        (def aggregate (d/load-aggregate-es- storage :vessel (:a-id t)))
        (def events (s/stream- storage (:a-id t) true))
        (def commands (s/stream- storage (:a-id t) false))
        (def num-events (count events))
        (def num-commands (count commands))
        


        (recur (rest tests)
               (conj with-metadata (assoc t
                                          :size-of-aggregate-bytes (mm/measure aggregate :bytes true)
                                          :entities-in-aggregate (+ 1 (count (:positions aggregate)) (count (:periods aggregate)))
                                          :num-events num-events
                                          :event-types (frequencies (map #(:n %) events))
                                          :size-of-events-bytes (mm/measure events :bytes true)
                                          :num-commands num-commands
                                          :size-of-commands-bytes (mm/measure commands :bytes true)
                                          :command-types (frequencies (map #(keyword (name (:n %))) commands))
                                          :average-load-times-ms (stats/mean (:load-times-ms t))
                                          :events-loaded-per-s-if-event-sourcing (int (/ 1000 (/ (stats/mean (:load-times-ms t)) num-events)))
                                          :commands-loaded-per-s-if-command-sourcing (int (/ 1000 (/ (stats/mean (:load-times-ms t)) num-commands)))
                                          :load-times-ms nil ; remove to show all loading times
                                          ))
               )))))

(defn run-single-store-tests! [config storage all-commands prefn postfn]
  (println "============ Start" (:type storage) "testing ============")

;  (if (not (nil? prefn)) (prefn))
  ;prepare - keep existing commands or produce new onew
  (if (not (nil? all-commands)) (do
                                            (println "Clearing out event store" (:type storage))
                                            (d/reset-aggregate-cache!)
                                            (reset-event-store! storage)
                                            (println "Executing commands on event store ...this might take some time..." (:type storage))
                                            (def command-time (b/benchmark (dorun (map #(d/dispatch-aggregate-cache! storage (:a-type %) (:a-id %) (:command %) (:p %)) all-commands))))
                                            (println "Executing commands on " (:type storage) " took " command-time)
                                            (println "Backing up events" (:type storage))
                                            (s/backup-event-store! storage)))

  (def res {:no-pruning {:aggregates (add-aggregate-metadata- storage (run-no-pruning-tests! config storage) true) }
   :superseeded {:aggregates (add-aggregate-metadata- storage (run-superseeded-tests! config storage) true) }
   :bounded {:aggregates (add-aggregate-metadata- storage (run-bounded-tests! config storage) true) }
   :probabilistic {:aggregates (add-aggregate-metadata- storage (run-probabilistic-tests! config storage) true) }
   :hierarchical {:aggregates (add-aggregate-metadata- storage (run-hierarchical-tests! config storage) true) }
   :full-snapshot {:aggregates (add-aggregate-metadata- storage (run-full-snapshot-tests! config storage) true) }
   :command-sourcing {:aggregates (add-aggregate-metadata- storage (run-command-sourcing-tests! config storage) false) }
            })

 ; (if (not (nil? postfn)) (postfn))
  res
 )

(defn test-on-new-data-set! [config]
  (println ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  (println ">>>>>>>>>>>>>>>>>>>>>>>>>>>>> Starting test-on-new-data-set! >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  (println ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  (println "Resetting projections storage")
  (reset-projections-store! (:projections-storage config))
  (println "Generating commands")
  (def all-commands (generate-commands config))
  {:metadata {:num-fixtures (reduce #(if (= (:command %2) "aggregate.fixture/create-fixture") (inc %1) %1) 0 all-commands)}
   :mongodb (run-single-store-tests! config (:storage-mongodb config) all-commands nil nil)
   :mysql (run-single-store-tests! config (:storage-mysql config) all-commands nil nil)
   :multi-file-edn (run-single-store-tests! config (:storage-multi-file-edn config) all-commands nil nil)
   }
 )

(defn write-test-results! [data config]
  (def output-fn (str (:test-output-dir config) "/" (ut/timestamp) ".edn"))
  (def output-fn2 (str (:test-output-dir config) "/" (ut/timestamp) "-params.edn"))
  (io/make-parents output-fn)
  (spit output-fn (pr-str data))
  (spit output-fn2 (pr-str config))
  (clojure.pprint/pprint config)
  (clojure.pprint/pprint data)
;  (println "multi-file-edn" (get-in data [:multi-file-edn :no-pruning :aggregates 0 :num-events]) (get-in data [:multi-file-edn :no-pruning :aggregates 0 :average-load-times-ms]) (:ais-event-resolution test-config))
;  (println "mongodb" (get-in data [:mongodb :no-pruning :aggregates 0 :num-events]) (get-in data [:mongodb :no-pruning :aggregates 0 :average-load-times-ms]) (:ais-event-resolution test-config))
;  (println "mysql" (get-in data [:mysql :no-pruning :aggregates 0 :num-events]) (get-in data [:mysql :no-pruning :aggregates 0 :average-load-times-ms]) (:ais-event-resolution test-config))
  (println "Output written to" output-fn "and" output-fn2)
  )

(defn execute! [config]
  (doall (repeatedly (:random-data-sets config)
                     #(write-test-results! (test-on-new-data-set! config) config)
                     ))
  (println "Done!")
)

(defn help []
  (println "Pass test configuration as EDN: <- disabled.. reads internal config")
  (println "Example: ")
  (clojure.pprint/pprint test-config))
           
(defn -main [& args]
  (if (nil? args) (help)
      (execute! test-config)))
