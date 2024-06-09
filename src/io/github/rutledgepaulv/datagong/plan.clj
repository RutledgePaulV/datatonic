(ns io.github.rutledgepaulv.datagong.plan
  (:require [clojure.set :as sets]
            [io.github.rutledgepaulv.datagong.utils :as utils]
            [io.github.rutledgepaulv.datagong.index :as index]))

(defmulti plan
  (fn [db bindings node]
    (utils/classify-clause node)))

(defmethod plan :default [db bindings clause]
  (throw (ex-info "Unsupported query clause." {:clause clause})))

(defmethod plan :pattern [db bindings clause]
  (let [clause-as-map    (zipmap (:components db) clause)
        logic-bindings   (index/get-logic-vars clause-as-map)
        reversed         (utils/reverse-map logic-bindings)
        const-bindings   (index/get-constants clause-as-map)
        const-keys       (set (keys const-bindings))
        logic-keys       (set (keys logic-bindings))
        logic-vars       (set (vals logic-bindings))
        bound-logic-vars (sets/intersection bindings logic-vars)]
    (let [input-positions   (into const-keys (map reversed) bound-logic-vars)
          scenario          {:in input-positions :out logic-keys}
          candidate-indices (get (:router db) scenario)
          selected-index    (first candidate-indices)]
      [:search
       {:index selected-index
        :in    (merge const-bindings (utils/reverse-map (select-keys reversed bound-logic-vars)))
        :out   logic-bindings}])))

(defmethod plan :predicate [db bindings clause]
  [:predicate
   {:in (utils/logic-vars (first clause))
    :fn (ffirst clause)}])

(defmethod plan :binding [db bindings clause]
  [:binding
   {:in  (utils/logic-vars (first clause))
    :fn  (ffirst clause)
    :out (utils/logic-vars (second clause))}])

(defmethod plan :and [db bindings [_ & children :as clause]]
  (let [{statement :statement outputs :bindings}
        (reduce (fn [agg clause]
                  (let [[kind attrs :as statement] (plan db (:bindings agg) clause)]
                    (cond->
                      agg
                      (= :search kind)
                      (update :bindings into (vals (:out attrs)))
                      (not= :search kind)
                      (update :bindings into (:out attrs))
                      :always
                      (update :statement conj statement))))
                {:bindings  bindings
                 :statement []}
                children)]
    (into [:and {:in bindings :out outputs}] statement)))

(defmethod plan :or [db bindings [_ & children :as clause]]
  )

(defmethod plan :not [db bindings [_ & children :as clause]]
  )

(defmethod plan :or-join [db bindings [_ & children :as clause]]
  )

(defmethod plan :and-join [db bindings [_ & children :as clause]]
  )

(defmethod plan :not-join [db bindings [_ & children :as clause]]
  )

(defmethod plan :rule [db bindings clause]
  )

(defn plan* [db clauses]
  (plan db #{} (if (list? clauses) clauses (cons 'and clauses))))


(comment

  (plan* (index/new-db)
         '[[?e :person/name "David"]
           [?e :person/age ?age]])

  #_[:and {:in #{}, :out #{?age ?e}}
     [:search {:index [:e :a :v], :in {:e :person/name, :a "David"}, :out {:v ?e}}]
     [:search {:index [:v :e :a], :in {:e :person/age, :v ?e}, :out {:v ?e, :a ?age}}]]

  )
