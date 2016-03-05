(ns parquet-clj.filter
  (:import [org.apache.parquet.filter2.predicate FilterApi Operators$Column FilterPredicate]
           [org.apache.parquet.filter2.compat FilterCompat FilterCompat$Filter])
  (:refer-clojure :exclude [= > >= < <= and or not]))


(defn int-column [col] (FilterApi/intColumn col))
(defn long-column [col] (FilterApi/longColumn col))
(defn float-column [col] (FilterApi/floatColumn col))
(defn double-column [col] (FilterApi/doubleColumn col))
(defn boolean-column [col] (FilterApi/booleanColumn col))
(defn binary-column [col] (FilterApi/binaryColumn col))

; seems like a good place for a macro: (defpredicate eq FilterApi/eq)
; or - even more terse (defpredicates eq FilterApi/eq
;                                     ne FilterApi/notEq
;                                     .. ...)
; to magical?
;   - or -
; to much boilerplate?
;
; that is the question...

(defn eq ^FilterPredicate [^Operators$Column column value] (FilterApi/eq column value))
(defn ne ^FilterPredicate [^Operators$Column column value] (FilterApi/notEq column value))
(defn lt ^FilterPredicate [^Operators$Column column value] (FilterApi/lt column value))
(defn le ^FilterPredicate [^Operators$Column column value] (FilterApi/ltEq column value))
(defn gt ^FilterPredicate [^Operators$Column column value] (FilterApi/gt column value))
(defn ge ^FilterPredicate [^Operators$Column column value] (FilterApi/gtEq column value))

(defn and ^FilterPredicate [^FilterPredicate left ^FilterPredicate right] (FilterApi/and left right))
(defn or ^FilterPredicate [^FilterPredicate left ^FilterPredicate right] (FilterApi/or left right))
(defn not ^FilterPredicate [^FilterPredicate predicate] (FilterApi/not predicate))

(def = eq)
(def != ne)
(def > gt)
(def >= ge)
(def < lt)
(def <= le)


(def && and)
(def || or)
(def ! not)

(defn predicate->filter ^FilterCompat$Filter [^FilterPredicate predicate] (FilterCompat/get predicate))