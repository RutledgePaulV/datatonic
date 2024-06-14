(ns io.github.rutledgepaulv.datatonic.algebra
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

(defn empty-rel? [rel]
  (or (empty? (:attrs rel)) (empty? (:tuples rel))))

(defn union-compatible? [rel1 rel2]
  (= (:attrs rel1) (:attrs rel2)))

(defn intersects? [s1 s2]
  (let [[larger smaller] (if (< (count s1) (count s2)) [s2 s1] [s1 s2])]
    (reduce (fn [nf x] (if (contains? larger x) (reduced true) nf)) false smaller)))

(defn restriction [rel pred]
  (update rel :tuples (fn [tuples] (into #{} (filter pred) tuples))))

(defn projection [rel attrs]
  (if (intersects? (:attrs rel) attrs)
    {:attrs  (sets/intersection (:attrs rel) attrs)
     :tuples (into #{} (map #(select-keys % attrs)) (:tuples rel))}
    (create-relation)))

(defn cartesian-product [rel1 rel2]
  {:attrs  (sets/union (:attrs rel1) (:attrs rel2))
   :tuples (into #{} (for [[x y] (** (remove empty? [(:tuples rel1) (:tuples rel2)]))]
                       (merge x y)))})

(defn union [rel1 rel2]
  {:attrs  (sets/union (:attrs rel1) (:attrs rel2))
   :tuples (sets/union (:tuples rel1) (:tuples rel2))})

(defn difference [rel1 rel2]
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
            (into {} (map (fn [x] [(select-keys x join-attrs) x])) (:tuples smaller))]
        {:attrs  (sets/union (:attrs rel1) (:attrs rel2))
         :tuples (into #{} (keep (fn [x] (when-some [match (get table (select-keys x join-attrs))]
                                           (merge x match))))
                       (:tuples larger))}))))

(defn intersection [rel1 rel2]
  {:attrs  (:attrs rel1)
   :tuples (sets/intersection (:tuples rel1) (:tuples rel2))})

(defn rename [rel1 renames]
  {:attrs  (into #{} (map (fn [x] (get renames x x))) (:attrs rel1))
   :tuples (into #{} (map (fn [m] (clojure.set/rename-keys m renames))) (:tuples rel1))})

