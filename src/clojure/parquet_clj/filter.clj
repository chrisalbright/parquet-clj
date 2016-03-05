(ns parquet-clj.filter
  (:import [org.apache.parquet.filter2.predicate FilterApi Operators$Column]
           [org.apache.parquet.filter2.compat FilterCompat]))


(defn long-column [col] (FilterApi/longColumn col))


(defn eq [^Operators$Column column value]
  (FilterApi/eq column value))


(defn predicate->filter [predicate] (FilterCompat/get predicate))