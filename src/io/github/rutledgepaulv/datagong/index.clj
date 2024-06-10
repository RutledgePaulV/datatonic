(ns io.github.rutledgepaulv.datagong.index
  (:require [clojure.set :as sets]
            [clojure.string :as str]
            [io.github.rutledgepaulv.datagong.utils :as utils]
            [me.tonsky.persistent-sorted-set :as pss]))

(defn safe-value [x]
  [(str (class x)) (hash x)])

(defn safe-compare [order a b]
  (loop [[k & more] order]
    (let [result
          (let [a' (get a k)
                b' (get b k)]
            (try
              (compare a' b')
              (catch Exception e
                (compare (safe-value a') (safe-value b')))))]
      (if (zero? result)
        (if (empty? more) result (recur more))
        result))))

(defn create-index [order]
  {:order order
   :data  (pss/sorted-set-by (partial safe-compare order))
   :name  (keyword (str/join (map name order)))})

(defn all-orderings [components]
  (->> (utils/permuted-powerset components)
       (mapcat identity)
       (map vec)))

(defn create-indices [orderings]
  (->> orderings
       (reduce
         (fn [agg order]
           (let [index-name (keyword (str/join (map name order)))]
             (assoc agg index-name (create-index order))))
         (sorted-map))))

(defn score-index-for-input [max-size ordering input]
  (->> (map vector ordering (range))
       (reduce
         (fn [score [component index]]
           (let [factor   (- max-size index)
                 operator (if (contains? input component) + -)]
             (operator score (bit-shift-left 1 factor))))
         0)))

(defn available-indices [all-indices input output]
  (let [largest-index (apply max (map count all-indices))]
    (->> all-indices
         (filter (fn [ordering] (sets/subset? (sets/union input output) (set ordering))))
         (into (sorted-set-by (fn [a b] (compare [(score-index-for-input largest-index b input) b]
                                                 [(score-index-for-input largest-index a input) a])))))))

(defn create-router
  "Given a set of orderings produce a router of input+output => ranked set of candidate indices (best to worst)"
  [index-orderings]
  (let [ps (utils/powerset (into #{} (mapcat identity) index-orderings))]
    (into {} (for [in ps out ps] [{:in in :out out} (available-indices index-orderings in out)]))))

(defn new-db
  "Create a new database. Creates all possible indices by
   default but can be customized to create specific indices
   to decrease storage usage. Queries use the best available
   index in each case."
  ([] (new-db [:e :a :v] (all-orderings [:e :a :v])))
  ([components orderings]
   (let [indices (create-indices orderings)
         router  (create-router orderings)]
     {:components components
      :indices    indices
      :router     router})))

(defn add-datom [db datom]
  (reduce
    (fn [agg k]
      (update-in
        agg [:indices k]
        (fn [{:keys [order] :as index}]
          (update index :data conj (select-keys (zipmap (:components db) datom) order)))))
    db
    (keys (get-in db [:indices]))))

(defn execute-search [db index inputs outputs]
  (let [partial-order (->> (get-in db [:indices index :order])
                           (take-while (fn [component] (contains? inputs component))))
        comparator    (partial safe-compare partial-order)]
    {:attrs  (set (vals outputs))
     :tuples (set (for [match
                        (->> (if (not-empty inputs)
                               (let [start-key (select-keys inputs partial-order)
                                     stop-key  (select-keys inputs partial-order)]
                                 (pss/slice (get-in db [:indices index :data]) start-key stop-key comparator))
                               (get-in db [:indices index :data]))
                             (filter (fn [datom] (= inputs (select-keys datom (keys inputs))))))]
                    (reduce (fn [agg [k v]] (assoc agg v (get match k))) {} outputs)))}))

(defn filter-vals [pred m]
  (reduce (fn [agg [k v]] (if (pred v) (assoc agg k v) agg)) {} m))

(defn get-logic-vars [m]
  (filter-vals utils/logic-var? m))

(defn get-constants [m]
  (filter-vals (complement utils/logic-var?) m))


(comment

  (new-db
    [:e :a :v]
    [[:e :a :v]
     [:a :e :v]
     [:a :v :e]
     [:v :a :e]])

  )
