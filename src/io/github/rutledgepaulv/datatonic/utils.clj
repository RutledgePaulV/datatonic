(ns io.github.rutledgepaulv.datatonic.utils
  (:require [clojure.math.combinatorics :as combos]))

(defn logic-var? [x]
  (and (symbol? x) (clojure.string/starts-with? (name x) "?")))

(defn wildcard? [x]
  (= '_ x))

(defn splat? [x]
  (and (vector? x)
       (= 2 (count x))
       (= '... (second x))))

(defn constant? [x]
  (and (not (wildcard? x)) (not (logic-var? x))))

(defn logic-vars [x]
  (into #{} (filter logic-var?) (tree-seq coll? seq x)))

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

(defn ensure-resolved [symbol]
  (if (qualified-symbol? symbol) symbol
    (.toSymbol (ns-resolve (the-ns 'clojure.core) symbol))))

(defn destruct
  "Given a binding target and a value return a set of maps."
  [binding value]
  (if (splat? binding)
    (->> (for [v value] (destruct (first binding) v))
         (mapcat identity)
         (into #{}))
    (let [logic-vars (logic-vars binding)]
      (-> (eval `(let [~binding ~value] ~(into {} (mapv (fn [k] [(keyword k) k]) logic-vars))))
          (update-keys symbol)
          (hash-set)))))

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
