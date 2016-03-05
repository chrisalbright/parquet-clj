(ns parquet-clj.read
  (:require [parquet-clj.filter :as filter])
  (:import (org.apache.parquet.hadoop ParquetReader)
           (com.cj.data.parquet ParquetIterable)
           (com.cj.data.parquet.clojure ClojureReadSupport)))

(defn- read-support []
  (ClojureReadSupport.))

(defn- parquet-reader
  ([path] (.. (ParquetReader/builder (read-support) path) build))
  ([path predicate] (.. (ParquetReader/builder (read-support) path) (withFilter (filter/predicate->filter predicate)) build)))

(defn parquet-seq
  ([path] (seq (ParquetIterable. (parquet-reader path))))
  ([path predicate] (seq (ParquetIterable. (parquet-reader path predicate)))))