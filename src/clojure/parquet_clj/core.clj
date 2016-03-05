(ns parquet-clj.core
  (:require [hdfs.core :as hdfs]
            [parquet-clj.read :as read]
            [parquet-clj.filter :as filter]))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]

  (let [path (hdfs/make-path "/Users/chris/Downloads/000000000536203187-000000000536211414.parquet")
        enterpriseId (filter/long-column "advertiserEnterpriseId")
        predicate (filter/eq enterpriseId 1501378)
        records (read/parquet-seq path predicate)]

    (doseq [record records] (prn record))))
