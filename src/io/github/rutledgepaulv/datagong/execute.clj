(ns io.github.rutledgepaulv.datagong.execute
  (:require [io.github.rutledgepaulv.datagong.index :as index]
            [io.github.rutledgepaulv.datagong.utils :as utils]
            [io.github.rutledgepaulv.datagong.algebra :as algebra]))


(defmulti execute (fn [db relation node] (first node)))


(defmethod execute :and [db relation [_ props & children]]
  (reduce
    (fn [relation child]
      (algebra/join relation (execute db relation child)))
    relation
    children))

(defmethod execute :or [db relation [_ props & children]]
  (reduce
    (fn [relation' child]
      (let [child-rel (execute db relation child)]
        (algebra/union relation' child-rel)))
    relation
    children))

(defmethod execute :not [db relation [_ props & children]]
  )

(defmethod execute :and-join [db relation [_ props & children]]
  )

(defmethod execute :or-join [db relation [_ props & children]]
  )

(defmethod execute :not-join [db relation [_ props & children]]
  )

(defmethod execute :search [db relation [_ {:keys [index in out]}]]
  (if (algebra/intersects? (:attrs relation) (set (vals in)))
    (let [rev-logic (utils/reverse-map in)]
      (reduce (fn [relation binding]
                (let [binding-vars (into {} (map (juxt (comp rev-logic key) val)) binding)]
                  (algebra/join relation (index/execute-search db index (merge in binding-vars) out))))
              (algebra/create-relation)
              (:tuples (algebra/projection relation (set (vals in))))))
    (index/execute-search db index in out)))

(defmethod execute :binding [db relation [_ props & children]]
  )

(defmethod execute :predicate [db relation [_ props & children]]
  )

(defn execute* [db plan]
  (execute db (algebra/create-relation) plan))
