(ns io.github.rutledgepaulv.datatonic.execute
  "Given a database and a query plan, execute the plan and perform
   all the necessary algebra to produce a single relation."
  (:require [clojure.set :as sets]
            [io.github.rutledgepaulv.datatonic.index :as index]
            [io.github.rutledgepaulv.datatonic.utils :as utils]
            [io.github.rutledgepaulv.datatonic.algebra :as algebra]
            [io.github.rutledgepaulv.datatonic.dyno :as dyno]
            [io.github.rutledgepaulv.datatonic.plan :as plan]))

(defn dispatch [db relation node]
  (first node))

(defmulti execute #'dispatch)

(defmethod execute :default [db relation node]
  (throw (ex-info "Unsupported execution node." {:node node})))

(defmethod execute :and [db relation [_ {:keys [in out]} & children]]
  (algebra/projection
    (reduce (partial execute db) relation children)
    (sets/union in out)))

(defmethod execute :or [db relation [_ {:keys [in out]} & children]]
  (->> children
       (map (partial execute db relation))
       (reduce algebra/union)
       (algebra/join relation)))

(defmethod execute :not [db relation [_ {:keys [out]} child]]
  (algebra/difference relation (execute db relation child)))

(defmethod execute :search [db relation [_ {:keys [index in out]}]]
  (if (algebra/intersects? (:attrs relation) (set (vals in)))
    (let [rev-logic        (sets/map-invert in)
          input-logic-vars (utils/logic-vars in)
          bindings         (:tuples (algebra/projection relation input-logic-vars))]
      (if (empty? bindings)
        (algebra/empty relation)
        (->> bindings
             (map (fn [binding]
                    (let [binding-vars (into {} (map (juxt (comp rev-logic key) val)) binding)]
                      (index/execute-search db index (merge in binding-vars) out))))
             (reduce algebra/union)
             (algebra/join relation))))
    (algebra/join relation (index/execute-search db index in out))))

(defmethod execute :binding [_ relation [_ {:keys [in out fn args out-pattern]}]]
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

(defmethod execute :predicate [db relation [_ {:keys [in fn args]}]]
  (let [child {:attrs  in
               :tuples (set (for [binding (:tuples (algebra/projection relation in))
                                  :when (apply (requiring-resolve fn) (map #(get binding % %) args))]
                              binding))}]
    (algebra/join relation child)))

(def ^:dynamic *breadcrumbs* #{})

(defmethod execute :rule [db relation [_ {:keys [in out]} expression :as node]]
  ; detect when we've hit a fixed point meaning we recurred to the same rule
  ; with an input relation which has already been seen. At that point we won't
  ; learn anything new, so we return the input relation unmodified.
  (let [breadcrumb {:node node :relation relation}]
    (if (contains? *breadcrumbs* breadcrumb)
      relation
      ; otherwise, record the rule+relation that is being entered,
      ; plan the rule one level deeper until the next recur target,
      ; and execute the plan
      (binding [*breadcrumbs* (conj *breadcrumbs* breadcrumb)]
        (execute db relation (plan/plan db in expression))))))

(defmethod execute :optimize [db relation [_ plan]]
  (execute db relation (dyno/optimize* db relation plan)))

(defn execute* [db plan]
  (execute db (algebra/create-relation) plan))
