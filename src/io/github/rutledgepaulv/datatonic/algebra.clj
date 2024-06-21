(ns io.github.rutledgepaulv.datatonic.algebra
  (:require [clojure.set :as sets])
  (:refer-clojure :exclude [empty]))

(defn create-relation
  ([] (create-relation #{} #{}))
  ([attrs tuples]
   {:attrs attrs :tuples tuples}))

(defn empty [rel]
  {:attrs (:attrs rel) :tuples #{}})

(defn **
  ([cols] (** '([]) cols))
  ([samples cols]
   (if (empty? cols)
     samples
     (recur (mapcat #(for [item (first cols)]
                       (conj % item)) samples)
            (rest cols)))))

(defn- empty-rel? [rel]
  (or (empty? (:attrs rel)) (empty? (:tuples rel))))

(defn- union-compatible? [rel1 rel2]
  (= (:attrs rel1) (:attrs rel2)))

(defn intersects? [s1 s2]
  (let [[larger smaller] (if (< (count s1) (count s2)) [s2 s1] [s1 s2])]
    (reduce (fn [nf x] (if (contains? larger x) (reduced true) nf)) false smaller)))

(defn- must-select-keys [m attrs]
  (let [result (select-keys m attrs)]
    (when (= (count result) (count attrs))
      result)))

(defn projection [rel attrs]
  (cond
    (= (:attrs rel) attrs)
    rel
    (intersects? (:attrs rel) attrs)
    {:attrs  attrs
     :tuples (into #{} (keep #(must-select-keys % attrs)) (:tuples rel))}
    :else
    (create-relation attrs #{})))

(defn cartesian-product [rel1 rel2]
  (cond
    (empty-rel? rel1)
    rel2
    (empty-rel? rel2)
    rel1
    :else
    (let [attrs (sets/union (:attrs rel1) (:attrs rel2))]
      {:attrs  attrs
       :tuples (into #{} (for [[x y] (** [(:tuples rel1) (:tuples rel2)])]
                           (merge x y)))})))

(defn union [rel1 rel2]
  (assert (union-compatible? rel1 rel2))
  {:attrs  (sets/union (:attrs rel1) (:attrs rel2))
   :tuples (sets/union (:tuples rel1) (:tuples rel2))})

(defn difference [rel1 rel2]
  (assert (union-compatible? rel1 rel2))
  {:attrs  (:attrs rel1)
   :tuples (sets/difference (:tuples rel1) (:tuples rel2))})

(defn join [rel1 rel2]
  (let [join-attrs (sets/intersection (:attrs rel1) (:attrs rel2))]
    (if (empty? join-attrs)
      (cartesian-product rel1 rel2)
      (let [[larger smaller]
            (if (< (count (:tuples rel1)) (count (:tuples rel1)))
              [rel1 rel2]
              [rel2 rel1])
            table
            (persistent!
              (reduce
                (fn [agg x]
                  (if-some [key (must-select-keys x join-attrs)]
                    (assoc! agg key (conj (get agg key #{}) x))
                    agg))
                (transient {})
                (:tuples smaller)))]
        {:attrs  (sets/union (:attrs rel1) (:attrs rel2))
         :tuples (persistent!
                   (reduce
                     (fn [agg x]
                       (if-some [key (must-select-keys x join-attrs)]
                         (if-some [matches (get table key)]
                           (reduce conj! agg (map (partial merge x) matches))
                           agg)
                         agg))
                     (transient #{})
                     (:tuples larger)))}))))
