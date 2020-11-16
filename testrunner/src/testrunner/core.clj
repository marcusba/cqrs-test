(ns testrunner.core (:require
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


;helper functions

(defn rand-range-int [from to] "Rand within interval. Inc from exc to" (+ (rand-int (- to from)) from))
(defn rand-range [from to] "Rand within interval. Inc from exc to" (+ (rand (- to from)) from))

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


                                        ;missing parameters.. clustering to certain vessels
                                      ; large events.. how
                                        ; vessels with no jobs hav noe idle periods either.. but they are maybe not so interesting.. ?
                                        ; just one idle period between jobs.. too little?.. splitting if larger than 14 days
                                        ; fixture distribution winter: 1,2,12 spring: 3,4,5 summer: 6,7,8 autumn: 9,10,11
;execute on multiple cores?
; 

(def test-config
  {
   ;simulation 
   :tmp-num-vessels (rand-range-int 120 180) ; num vessels to be generated
   :num-vessels 1
   :tmp2-num-vessels 150
   :preferred-vessels [40 1.3] ; 40% of vessels are preffered with weight at 1.3
   :x-vessel-age [80 24 100 12] ; vessel age (determines if a vessel can be assigned a job) distribution 80% are 24 months old, 20% are 12 months old.
   :old-fixtures-per-month (vec (repeat 24 2))
   :fixtures-per-month [1]
   :vessel-age [100 24]

   :tmp-fixtures-per-month [(rand-range-int 15 30) ;january year 1
                        (rand-range-int 15 30) ;february
                        (rand-range-int 20 35) ;march
                        (rand-range-int 20 35) ;april
                        (rand-range-int 20 35) ;may
                        (rand-range-int 25 45) ;june
                        (rand-range-int 25 45) ;july
                        (rand-range-int 25 45) ;august
                        (rand-range-int 20 35) ;september
                        (rand-range-int 20 35) ;october
                        (rand-range-int 20 25) ;november
                        (rand-range-int 15 30) ;december
                        (rand-range-int 15 30) ;january year 2
                        (rand-range-int 15 30) ;february
                        (rand-range-int 20 35) ;march
                        (rand-range-int 20 35) ;april
                        (rand-range-int 20 35) ;may
                        (rand-range-int 25 45) ;june
                        (rand-range-int 25 45) ;july
                        (rand-range-int 25 45) ;august
                        (rand-range-int 20 35) ;september
                        (rand-range-int 20 35) ;october
                        (rand-range-int 20 35) ;november
                        (rand-range-int 15 30) ;december
                        ] ;(vec (repeat 24 2)) ; over 2 years
   :contract-lengths [80 14 100 3] ; job lengths 80% are 14 days long, 20% are 3 days long
   :ais-event-resolution 24 ; hours between each report
   :tmp-ais-event-resolution 720

   ;dates
   :start-date-time (t/zoned-date-time 2019 2 1) ; simulation starts 2019-02 at and ends at 2021-01 (including)
   :now-date-time (t/zoned-date-time 2020 12 31) ; things happening after this date is in the future (e.g. no AIS reports etc)

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
                            :directory (str (System/getProperty "user.home") "/data/event-store")}
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
   :tmp-random-data-sets 5 ;times the whole suite of tests will be run. i.e. how many distinct data sets we will generate
   :random-data-sets 1
   :tmp-test-iterations 1000 ;how many times each test will run for each pruning method e.g. loading an aggregate 1000 times
   :test-iterations 10
   :tmp-cooldown-time 300 ;sec delay before each test set (per pruning)
   :cooldown-time 1
   :test-output-dir (str (System/getProperty "user.home") "/data/testrunner")
   })

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

(defn generate-idle-commands [fixture prev-fixture vessel now-date-time]

  (def now-ut (ut/from-zoned-date-time now-date-time))
  (def from (let []
              (def tmp-from (if (nil? prev-fixture)
                (get-in vessel [:p :built])
                ( + 1 (get-in prev-fixture [:p :to]))))

              (if (> tmp-from now-ut) now-ut tmp-from)))

  (def to (let []
              (def tmp-to (- (get-in fixture [:p :from]) 1))
              (if (> tmp-to now-ut) now-ut tmp-to)))

  (def days-14 (* 86400000 14))

  (if (= from to)
    []
     (if (> (- to from) days-14) ;split in tow if over 14 days
       [(generate-period-command (get-in vessel [:p :id]) :vessel from (- (+ from days-14) 1) "Some first idle period")
        (generate-period-command (get-in vessel [:p :id]) :vessel (+ from days-14) to "Some other idle period")]
       
       (if (< (- to from) 0) []
           [(generate-period-command (get-in vessel [:p :id]) :vessel from to "Some idle period")])
       )))

(defn assign-fixture-to-vessel [fixture vessels vessel-fixtures preferred-vessels now-date-time]
  (loop [drawn-vessel (select-by-weight vessels (first preferred-vessels) (last preferred-vessels)) tries 0]
    (def drawn-vessel-id (get-in drawn-vessel [:p :id]))
    (def drawn-vessel-built (get-in drawn-vessel [:p :built]))
    (def fixtures-to-check (filter #(= drawn-vessel-id (get-in % [:p :vessel-id])) vessel-fixtures))
    (def overlapping-fixtures (filter (fn [f]
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
    (def all-commands (conj (generate-idle-commands fixture prev-fixture drawn-vessel now-date-time)
                            (generate-period-command drawn-vessel-id :fixture (get-in fixture [:p :from]) (get-in fixture [:p :to]) "")
                            (assoc-in fixture [:p :vessel-id] drawn-vessel-id)))
    (if (= tries (* (count vessels) 100)) (throw (Exception. "Could not assign all fixtures. No available vessel. Try again.")))
      

    (if (and (empty? overlapping-fixtures) (>= (get-in fixture [:p :from]) drawn-vessel-built)) ;update fixture command with ref to vessel.. ok if available + if old enough.. if not try again..
      (into vessel-fixtures all-commands)
      (recur (select-by-weight vessels (first preferred-vessels) (last preferred-vessels)) (inc tries))
      )))


(defn assign-fixtures-to-vessels [fixtures vessels preferred-vessels now-date-time]
  (loop [unassigned-fixtures fixtures         
         vessel-fixtures []]
    (println (count unassigned-fixtures) "fixtures left to assign to vessels")
    (if (empty? unassigned-fixtures)
      vessel-fixtures
      (recur (rest unassigned-fixtures) (assign-fixture-to-vessel (first unassigned-fixtures) vessels vessel-fixtures preferred-vessels now-date-time))
    )))

(defn generate-vessel-commands [num-vessels vessel-age now-date-time]
  (def ages (randomize-percentile vessel-age (range num-vessels)))
  (map (fn [vessel]
         {:a-type :vessel
          :a-id ""
          :command "aggregate.vessel/create-vessel"
          :p {:id (util.uuid/create-uuid-string)
              :name (str "Some vessel " vessel)
              :built (ut/from-zoned-date-time (t/adjust now-date-time t/minus (t/months (nth ages vessel))))}
           })
       (range num-vessels))
  )

(defn generate-fixture-commands [fixtures-per-month contract-lengths start-date-time]
  (def tot-fixtures (reduce + fixtures-per-month))
  (def durations (randomize-percentile contract-lengths (range tot-fixtures)))
  (def fixtures (map (fn [fixture]
        (let [duration (nth durations fixture)
              month (find-group (inc fixture) fixtures-per-month)
              from (t/adjust
                    (t/adjust start-date-time t/plus (t/months month))
                    t/plus (t/days (rand 30)))
              to (t/adjust (t/adjust from t/plus (t/days duration)) t/minus (t/millis 1))]
         {:a-type :fixture
          :a-id ""
          :command "aggregate.fixture/create-fixture"
          :p {:id (util.uuid/create-uuid-string)
              :from (ut/from-zoned-date-time from)
              :to (ut/from-zoned-date-time to)
              }
          }))
          (range tot-fixtures)))
  fixtures)

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

(defn generate-ais-report-commands [vessels ais-event-resolution start-date-time now-date-time]

  (def start (ut/from-zoned-date-time start-date-time))
  (def end (ut/from-zoned-date-time now-date-time))
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

(defn generate-commands [{:keys [num-vessels vessel-age fixtures-per-month contract-lengths start-date-time now-date-time preferred-vessels ais-event-resolution port-calls-per-fixture]}]
  
  (def vessels (generate-vessel-commands num-vessels vessel-age now-date-time))
  (println (count vessels) "vessels generated")
  (def fixtures (generate-fixture-commands fixtures-per-month contract-lengths start-date-time))
  (println (count fixtures) "fixtures generated")  
  (def assigned-fixtures (assign-fixtures-to-vessels fixtures vessels preferred-vessels now-date-time))
  (println (count fixtures) "fixtures assigned")
  (println (- (count assigned-fixtures) (count fixtures)) "vessel periods generated")  
  (def ais-reports (generate-ais-report-commands vessels ais-event-resolution start-date-time now-date-time))
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
  (def pruning-data (p/prune-bounded! storage (:projections-storage config) (:bounded-buffer config) vessels))
  (cooldown (:cooldown-time config)) 
  (println "Running probabilistic tests" (:type storage))  
  (run-single-tests- storage true (:test-iterations config) vessels pruning-data)
  )

(defn run-hierarchical-tests! [config storage]
  (s/restore-event-store! storage)
  (def vessels (qv/get-all-vessel-ids- (:projections-storage config)))
  (println "Pruning for hierarchical tests")
  (def pruning-data (p/prune-hierarchical! storage (:projections-storage config) (ut/from-zoned-date-time (:now-date-time config)) (:hierarchical-windows config) vessels))
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

        (recur (rest tests)
               (conj with-metadata (assoc t
                                          :size-of-aggregate-bytes (mm/measure aggregate :bytes true)
                                          :entities-in-aggregate (+ 1 (count (:positions aggregate)) (count (:periods aggregate)))
                                          :num-events (count events)
                                          :event-types (frequencies (map #(:n %) events))
                                          :size-of-events-bytes (mm/measure events :bytes true)
                                          :num-commands (count commands)
                                          :size-of-commands-bytes (mm/measure commands :bytes true)
                                          :command-types (frequencies (map #(keyword (name (:n %))) commands))
                                          :average-load-times-ms (stats/mean (:load-times-ms t))
                                          ))
               )))))

(defn run-single-store-tests! [config storage all-commands prefn postfn]
  (println "============ Start" (:type storage) "testing ============")

;  (if (not (nil? prefn)) (prefn))
  ;prepare - keep existing commands or produce new onew
  (if (not (nil? all-commands)) (do
                                            (println "Clearing out event store" (:type storage))
                                            (reset-event-store! storage)
                                            (println "Executing commands on event store ...this might take some time..." (:type storage))
                                            (dorun (map #(d/dispatch-es! storage (:a-type %) (:a-id %) (:command %) (:p %)) all-commands))
                                            (println "Backing up events" (:type storage))
                                            (s/backup-event-store! storage)))

  (def res {;:no-pruning {:aggregates (add-aggregate-metadata- storage (run-no-pruning-tests! config storage) true) }
   ;:superseeded {:aggregates (add-aggregate-metadata- storage (run-superseeded-tests! config storage) true) }
   ;:bounded {:aggregates (add-aggregate-metadata- storage (run-bounded-tests! config storage) true) }
   ;:probabilistic {:aggregates (add-aggregate-metadata- storage (run-probabilistic-tests! config storage) true) }
   ;:hierarchical {:aggregates (add-aggregate-metadata- storage (run-hierarchical-tests! config storage) true) }
   ;:full-snapshot {:aggregates (add-aggregate-metadata- storage (run-full-snapshot-tests! config storage) true) }
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
   :mongodb (run-single-store-tests! config (:storage-mongodb config) all-commands #(sh "/usr/bin/sudo" "/usr/sbin/service" "mongod" "start") #(sh "/usr/bin/sudo" "/usr/sbin/service" "mongod" "stop"))
   :mysql (run-single-store-tests! config (:storage-mysql config) all-commands nil nil)
   :multi-file-edn (run-single-store-tests! config (:storage-multi-file-edn config) all-commands nil nil)
   }
 )

(defn test-on-existing-data-set! [config]
  (println ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  (println ">>>>>>>>>>>>>>>>>>>>>>>>>> Starting test-on-existing-data-set! >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  (println ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  (sh "/usr/bin/sudo" "/usr/sbin/service" "mongod" "start")
  (s/restore-event-store! (:storage-mysql config))
  (s/restore-event-store! (:storage-mongodb config))
  (s/restore-event-store! (:storage-multi-file-edn config))

  {:metadata nil
   :mongodb (run-single-store-tests! config (:storage-mongodb config) all-commands #(sh "/usr/bin/sudo" "/usr/sbin/service" "mongod" "start") #(sh "/usr/bin/sudo" "/usr/sbin/service" "mongod" "stop"))
;   :mysql (run-single-store-tests! config (:storage-mysql config) nil nil nil)
;   :multi-file-edn (run-single-store-tests! config (:storage-multi-file-edn config) nil nil nil)
   }
 )

(defn write-test-results! [data directory]
  (def output-fn (str directory "/" (ut/timestamp) ".edn"))
  (io/make-parents output-fn)
  (spit output-fn (pr-str data))
  (println "Output written to" output-fn)
  )

(defn execute! [config]
  (doall (repeatedly (:random-data-sets config)
                     #(write-test-results! (test-on-new-data-set! config) (:test-output-dir config))
                     ))
  (println "Done!")
)

(defn execute-existing! [config]
  (write-test-results! (test-on-existing-data-set! config) (:test-output-dir config))
  (println "Done!")
)

(defn help []
  (println "Pass test configuration as EDN: <- disabled.. reads internal config")
  (println "Example: ")
  (clojure.pprint/pprint test-config))
           
(defn -main [& args]
  (if (nil? args) (help)
      (execute! (clojure.edn/read-string (first args)))))
