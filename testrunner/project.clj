(defproject testrunner "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.namespace "0.3.1"]
                 [event-store "0.1.0-SNAPSHOT"]
                 [pruning "0.1.0-SNAPSHOT"]
                 [command "0.1.0-SNAPSHOT"]
                 [util "0.1.0-SNAPSHOT"]
                 [clojure.java-time "0.3.2"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [com.clojure-goes-fast/clj-memory-meter "0.1.3"]
                 [net.mikera/core.matrix "0.62.0"]
                 ]
  :main testrunner.core
  :jvm-opts ^:replace ["-Djdk.attach.allowAttachSelf"]
  :repl-options {:init-ns testrunner.core})
