(ns io.github.rutledgepaulv.datatonic.execute
  "Given a database and a query plan, execute the plan and perform
   all the necessary algebra to produce a single relation."
  (:require [clojure.set :as sets]
            [io.github.rutledgepaulv.datatonic.index :as index]
            [io.github.rutledgepaulv.datatonic.utils :as utils]
            [io.github.rutledgepaulv.datatonic.algebra :as algebra]
            [io.github.rutledgepaulv.datatonic.dyno :as dyno]))

(defn dispatch [db relation node]
  (first node))

(defmulti execute #'dispatch)

(defmethod execute :default [db relation node]
  (throw (ex-info "Unsupported execution node." {:node node})))

(defmethod execute :and [db relation [_ props & children]]
  (reduce (partial execute db) relation children))

(defmethod execute :or [db relation [_ props & children]]
  (reduce
    (fn [relation' child]
      ; intentionally uses the original relation as basis
      (let [child-rel (execute db relation child)]
        (algebra/union relation' child-rel)))
    relation
    children))

(defmethod execute :not [db relation [_ props child]]
  (algebra/difference relation (algebra/join relation (execute db relation child))))

(defmethod execute :search [db relation [_ {:keys [index in out]}]]
  (algebra/join
    relation
    (if (algebra/intersects? (:attrs relation) (set (vals in)))
      (let [rev-logic        (sets/map-invert in)
            input-logic-vars (utils/logic-vars in)]
        (reduce (fn [relation binding]
                  (let [binding-vars (into {} (map (juxt (comp rev-logic key) val)) binding)]
                    (algebra/union relation (index/execute-search db index (merge in binding-vars) out))))
                (algebra/create-relation)
                (:tuples (algebra/projection relation input-logic-vars))))
      (index/execute-search db index in out))))

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

(defmethod execute :predicate [db relation [_ {:keys [in out fn args]}]]
  (let [child {:attrs  (:attrs relation)
               :tuples (set (for [binding (:tuples relation)
                                  :when (apply (requiring-resolve fn) (map #(get binding % %) args))]
                              binding))}]
    (algebra/join relation child)))

(defmethod execute :rule [db relation [_ {:keys [in out]} expression]]
  )

(defmethod execute :optimize [db relation [_ plan]]
  (execute db relation (dyno/optimize* db relation plan)))

(defn execute* [db plan]
  (execute db (algebra/create-relation) plan))
