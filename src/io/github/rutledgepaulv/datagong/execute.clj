(ns io.github.rutledgepaulv.datagong.execute
  (:require [clojure.set :as sets]
            [io.github.rutledgepaulv.datagong.index :as index]
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


(defmethod execute :binding [db relation [_ {:keys [in out fn args out-pattern]}]]
  (let [child {:attrs  (sets/union in out)
               :tuples (set (for [binding (:tuples (algebra/projection relation in))
                                  output  (let [return-value (apply (requiring-resolve fn) (map #(get binding % %) args))]
                                            (cond
                                              (symbol? out-pattern)
                                              [{out-pattern return-value}]
                                              (and (vector? out-pattern) (vector? (first out-pattern)))
                                              (for [v return-value] (zipmap (first out-pattern) v))
                                              (and (vector? out-pattern) (coll? return-value))
                                              (zipmap out-pattern (first return-value))))]
                              (merge output binding)))}]
    (algebra/join relation child)))

(defmethod execute :predicate [db relation [_ props]]
  )

(defn execute* [db plan]
  (execute db (algebra/create-relation) plan))
