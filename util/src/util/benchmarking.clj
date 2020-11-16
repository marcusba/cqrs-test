(ns util.benchmarking)


(defmacro benchmark-ns
"Times the execution of forms, discarding their output and returning
a long in nanoseconds."
([& forms]
`(let [start# (System/nanoTime)]
~@forms
(- (System/nanoTime) start#))))

(defmacro benchmark-int
"Times the execution of forms, discarding their output and returning
a long in nanoseconds."
([& forms]
`(let [start# (System/nanoTime)]
~@forms
(Math/round (double (/ (- (System/nanoTime) start#) 1000000))))))

(defmacro benchmark
"Times the execution of forms, discarding their output and returning
a long in nanoseconds."
([& forms]
`(let [start# (System/nanoTime)]
~@forms
(/ (Math/round (*
                (double (/ (- (System/nanoTime) start#) 1000000))
                  (Math/pow 10 1))) (Math/pow 10 1))
)))


