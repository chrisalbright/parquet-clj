(defproject parquet-export-clj "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths      ["src/clojure"]
  :java-source-paths ["src/java"]

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [hdfs-clj "0.1.15"]
                 [org.apache.hadoop/hadoop-client "2.3.0-cdh5.1.0"]
                 [org.apache.parquet/parquet-format "2.3.1"]
                 [org.apache.parquet/parquet-tools "1.8.1"]
                 [org.apache.parquet/parquet-hadoop "1.8.1"]]

  :main ^:skip-aot parquet-clj.core

  :target-path "target/%s"

  :profiles {:uberjar {:aot :all}})
