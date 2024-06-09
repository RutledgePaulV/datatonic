(ns io.github.rutledgepaulv.datagong.core
  (:require [clojure.set :as sets]
            [clojure.string :as str]
            [me.tonsky.persistent-sorted-set :as pss]
            [io.github.rutledgepaulv.datagong.algebra :as algebra]
            [clojure.math.combinatorics :as combos]))

(def components
  {:e {:name "entity"}
   :a {:name "attribute"}
   :v {:name "value"}})

(defn powerset
  ([s] (powerset s [#{}]))
  ([s acc]
   (if (empty? s)
     acc
     (recur (rest s)
            (into acc (map #(conj % (first s)) acc))))))

(defn permuted-powerset [xs]
  (for [i (range (count xs))]
    (combos/permuted-combinations xs (inc i))))

(def indexes
  (->> (permuted-powerset (keys components))
       (mapcat identity)
       (reduce
         (fn [agg order]
           (let [index-name (keyword (str/join (map name order)))]
             (assoc agg index-name {:order (vec order) :name index-name})))
         (sorted-map))))

(defn safe-value [x]
  [(str (class x)) (hash x)])

(defn safe-compare [order a b]
  (reduce
    (fn [result k]
      (let [result'
            (try
              (compare (get a k) (get b k))
              (catch Exception e
                (compare (safe-value (get a k)) (safe-value (get b k)))))]
        (if (zero? result')
          result
          (reduced result'))))
    0
    order))

(defn new-db []
  (persistent!
    (reduce-kv
      (fn [agg k {:keys [order]}]
        (assoc! agg k (pss/sorted-set-by (partial safe-compare order))))
      (transient {})
      indexes)))

(defn add-datom [db datom]
  (persistent!
    (reduce-kv
      (fn [agg k v]
        (let [{:keys [order]} (get indexes k)]
          (assoc! agg k (conj v (select-keys (zipmap (keys components) datom) order)))))
      (transient {})
      db)))

(def index-selection
  (into {} (for [in  (powerset (set (keys components)))
                 out (powerset (set (keys components)))
                 :let [k       {:in in :out out}
                       options (filter
                                 (fn [[k v]]
                                   (and (= (set (:order v)) (sets/union in out))
                                        (= (set (take (count in) (:order v))) in)))
                                 indexes)]
                 :when (not-empty options)]
             [k (into #{} (map key options))])))

(defn logic-var? [x]
  (and (symbol? x) (clojure.string/starts-with? (name x) "?")))

(defn get-logic-vars [[e a v]]
  (cond-> {}
    (logic-var? e)
    (assoc :e e)
    (logic-var? a)
    (assoc :a a)
    (logic-var? v)
    (assoc :v v)))

(defn get-constants [[e a v]]
  (cond-> {}
    (and (some? e) (not (logic-var? e)))
    (assoc :e e)
    (and (some? a) (not (logic-var? a)))
    (assoc :a a)
    (and (some? v) (not (logic-var? v)))
    (assoc :v v)))

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

(defn execute-search [db index inputs outputs]
  (let [partial-order (->> (get-in indexes [index :order])
                           (take-while (fn [component] (contains? inputs component))))
        comparator    (partial safe-compare partial-order)]
    {:attrs  (set (vals outputs))
     :tuples (set (for [match
                        (if (not-empty inputs)
                          (let [start-key (select-keys inputs partial-order)
                                stop-key  (select-keys inputs partial-order)]
                            (pss/slice (get db index) start-key stop-key comparator))
                          (get db index))]
                    (reduce (fn [agg [k v]] (assoc agg v (get match k))) {} outputs)))}))

(defn greatest [indices]
  (reduce
    (fn [greatest-so-far x]
      (if (neg? (compare greatest-so-far x))
        x
        greatest-so-far))
    indices))

(defn dispatch [ctx clause]
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

(defmulti plan #'dispatch)

(defmethod plan :default [ctx clause]
  (throw (ex-info "Unsupported query clause." {:clause clause})))

(defmethod plan :pattern [{:keys [relation db] :as ctx} clause]
  (let [logic-vars (get-logic-vars clause)
        rev-logic  (reverse-map logic-vars)
        const-vars (get-constants clause)
        const-keys (set (keys const-vars))
        logic-keys (set (keys logic-vars))]
    (cond
      ; pattern is at least partially constrained by existing relation
      (algebra/intersects? (:attrs relation) (set (vals logic-vars)))
      (reduce (fn [relation binding]
                (let [binding-vars      (into {} (map (fn [[logic-var-name value]]
                                                        {(get rev-logic logic-var-name) value}))
                                              binding)
                      known-positions   (sets/union (set (keys binding-vars)) const-keys)
                      output-positions  (set (keys logic-vars))
                      candidate-indices (get index-selection {:in known-positions :out output-positions})
                      selected-index    (greatest candidate-indices)
                      search-relation   (execute-search db selected-index (merge binding-vars const-vars) logic-vars)]
                  (algebra/join relation search-relation)))
              (algebra/create-relation)
              (:tuples (algebra/projection relation (set (vals logic-vars)))))

      :else
      (let [candidate-indices (get index-selection {:in const-keys :out logic-keys})
            selected-index    (greatest candidate-indices)]
        (execute-search db selected-index const-vars logic-vars)))))

(defmethod plan :predicate [{:keys [relation] :as ctx} clause]
  (:relation ctx))

(defmethod plan :binding [{:keys [relation] :as ctx} clause]
  (:relation ctx))

(defmethod plan :and [{:keys [relation] :as ctx} [_ & children :as clause]]
  (reduce
    (fn [relation child]
      (algebra/join relation (plan (assoc ctx :relation relation) child)))
    relation
    children))

(defmethod plan :or [{:keys [relation] :as ctx} [_ & children :as clause]]
  (reduce
    (fn [relation' child]
      (algebra/union relation' (plan (assoc ctx :relation relation) child)))
    relation
    children))

(defmethod plan :not [ctx [_ & children :as clause]]
  (:relation ctx))

(defmethod plan :or-join [ctx [_ & children :as clause]]
  (:relation ctx))

(defmethod plan :and-join [ctx [_ & children :as clause]]
  (:relation ctx))

(defmethod plan :not-join [ctx [_ & children :as clause]]
  (:relation ctx))

(defmethod plan :rule [ctx clause]
  (:relation ctx))

(defn plan* [db clauses]
  (plan {:relation (algebra/create-relation) :db db}
        (if (list? clauses) clauses (cons 'and clauses))))
