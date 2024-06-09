(ns io.github.rutledgepaulv.datagong.utils
  (:require [clojure.math.combinatorics :as combos]))

(defn logic-var? [x]
  (and (symbol? x) (clojure.string/starts-with? (name x) "?")))

(defn logic-vars [x]
  (into #{} (filter logic-var?) x))

(defn reverse-map [x]
  (into {} (map (comp vec rseq vec)) x))

(defn pattern-clause? [x]
  (and (vector? x) (not (seq? (first x)))))

(defn binding-clause? [x]
  (and (vector? x) (= 2 (count x)) (seq? (first x))))

(defn predicate-clause? [x]
  (and (vector? x) (= 1 (count x)) (seq? (first x))))

(defn rule? [x]
  (and (seq? x) (symbol? (first x))))

(defn powerset
  ([s] (powerset s [#{}]))
  ([s acc] (if (empty? s) acc (recur (rest s) (into acc (map #(conj % (first s))) acc)))))

(defn permuted-powerset [xs]
  (for [i (range (count xs))] (combos/permuted-combinations xs (inc i))))

(defn classify-clause [clause]
  (cond
    (pattern-clause? clause)
    :pattern
    (binding-clause? clause)
    :binding
    (predicate-clause? clause)
    :predicate
    (rule? clause)
    (if (contains? #{'or 'or-join 'not-join 'and 'and-join 'not} (first clause))
      (keyword (first clause))
      :rule)))
