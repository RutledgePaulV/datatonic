(ns io.github.rutledgepaulv.datagong.algebra
  (:require [clojure.set :as sets]))

(defn create-relation
  ([] (create-relation #{} #{}))
  ([tuples]
   (if (not-empty tuples)
     (create-relation (set (keys (first tuples))) tuples)
     (create-relation)))
  ([attrs tuples]
   (assert (set? attrs) "must be a set.")
   (assert (set? tuples) "must be a set.")
   {:attrs attrs :tuples tuples}))

(defn **
  ([cols] (** '([]) cols))
  ([samples cols]
   (if (empty? cols)
     samples
     (recur (mapcat #(for [item (first cols)]
                       (conj % item)) samples)
            (rest cols)))))

(defn union-compatible? [rel1 rel2]
  (= (:attrs rel1) (:attrs rel2)))

(defn restriction [rel1 pred]
  (update rel1 :tuples (fn [tuples] (into #{} (filter pred) tuples))))

(defn projection [rel1 attrs]
  {:attrs rel1 :tuples (into #{} (map #(select-keys % attrs)) (:tuples rel1))})

(defn cartesian-product [rel1 rel2]
  {:attrs  (sets/union (:attrs rel1) (:attrs rel2))
   :tuples (into #{} (for [[x y] (** [(:tuples rel1) (:tuples rel2)])]
                       (merge x y)))})

(defn union [rel1 rel2]
  {:attrs  (sets/union (:attrs rel1) (:attrs rel2))
   :tuples (sets/union (:tuples rel1) (:tuples rel2))})

(defn difference [rel1 rel2]
  {:attrs  (sets/union (:attrs rel1) (:attrs rel2))
   :tuples (sets/difference (:tuples rel1) (:tuples rel2))})

(defn join [rel1 rel2]
  {:attrs  (sets/union (:attrs rel1) (:attrs rel2))
   :tuples (let [join-attrs (sets/intersection (:attrs rel1) (:attrs rel2))
                 [larger smaller] (if (< (count (:tuples rel1)) (count (:tuples rel1)))
                                    [rel1 rel2]
                                    [rel2 rel1])
                 table      (into {} (map (fn [x] [(select-keys x join-attrs) x])) (:tuples smaller))]
             (into #{} (keep (fn [x] (when-some [match (get table (select-keys x join-attrs))]
                                       (merge x match))))
                   (:tuples larger)))})

(defn intersection [rel1 rel2]
  {:attrs  (sets/union (:attrs rel1) (:attrs rel2))
   :tuples (sets/difference (:tuples rel1) (:tuples rel2))})

(defn rename [rel1 renames]
  {:attrs  (into #{} (map (fn [x] (get renames x x))) (:attrs rel1))
   :tuples (into #{} (map (fn [m] (clojure.set/rename-keys m renames))) (:tuples rel1))})

