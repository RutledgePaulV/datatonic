(ns io.github.rutledgepaulv.datatonic.plan
  "Given a database and a query, compute a query plan. This namespace
   prioritizes correctness and may emit seemingly naive plans.
   Performance is left as an exercise for the optimizer."
  (:require [clojure.set :as sets]
            [clojure.string :as str]
            [io.github.rutledgepaulv.datatonic.utils :as utils]
            [io.github.rutledgepaulv.datatonic.index :as index]))

(defn dispatch [db bindings node]
  (utils/classify-clause node))

(defmulti plan #'dispatch)

(defmethod plan :default [db bindings clause]
  (throw (ex-info "Unsupported query clause." {:clause clause})))

(defmethod plan :pattern [db bindings clause]
  (let [clause-as-map    (zipmap (:components db) clause)
        logic-bindings   (index/get-logic-vars clause-as-map)
        reversed         (sets/map-invert logic-bindings)
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
       {:index (keyword (str/join (map name selected-index)))
        :in    (merge const-bindings (sets/map-invert (select-keys reversed bound-logic-vars)))
        :out   logic-bindings}])))

(defmethod plan :predicate [db bindings clause]
  [:predicate
   {:in   (sets/intersection bindings (utils/logic-vars (first clause)))
    :args (vec (rest (first clause)))
    :fn   (utils/ensure-resolved (ffirst clause))
    :out  #{}}])

(defmethod plan :binding [db bindings clause]
  [:binding
   {:in          (sets/intersection bindings (utils/logic-vars (first clause)))
    :fn          (utils/ensure-resolved (ffirst clause))
    :args        (vec (rest (first clause)))
    :out         (utils/logic-vars (second clause))
    :out-pattern (second clause)}])

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
  (let [{statement :statement outputs :bindings}
        (reduce (fn [agg clause]
                  (let [[kind attrs :as statement] (plan db bindings clause)]
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
    (into [:or {:in bindings :out outputs}] statement)))

(defmethod plan :not [db bindings [_ child :as clause]]
  (let [child-plan             (plan db bindings child)
        child-input-logic-vars (into #{} (filter utils/logic-var?) (vals (:in (second child-plan))))]
    [:not {:in (sets/intersection bindings child-input-logic-vars) :out #{}} child-plan]))

(defmethod plan :or-join [db bindings [_ binding-vector & children :as clause]]
  (let [{statement :statement outputs :bindings}
        (reduce (fn [agg clause]
                  (let [[kind attrs :as statement] (plan db (sets/intersection bindings (set binding-vector)) clause)]
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
    (into [:or {:in bindings :out (sets/intersection outputs (set binding-vector))}] statement)))

(defmethod plan :and-join [db bindings [_ binding-vector & children :as clause]]
  (let [{statement :statement outputs :bindings}
        (reduce (fn [agg clause]
                  (let [[kind attrs :as statement] (plan db (sets/intersection (:bindings agg) (set binding-vector)) clause)]
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
    (into [:and {:in bindings :out (sets/intersection outputs (set binding-vector))}] statement)))

(defmethod plan :not-join [db bindings [_ binding-vector & children :as clause]]
  (let [child-plan             (plan db (sets/intersection bindings (set binding-vector)) (cons 'and children))
        child-input-logic-vars (into #{} (filter utils/logic-var?) (vals (:in (second child-plan))))]
    [:not {:in (sets/intersection bindings (set binding-vector) child-input-logic-vars) :out #{}} child-plan]))

(defmethod plan :rule [db bindings clause]
  (let [clauses (get-in db [:rules (first clause)])]))

(defn plan* [db clauses]
  (plan db #{} (if (list? clauses) clauses (cons 'and clauses))))


(comment

  (plan*
    (assoc (index/new-db) :rules rules)
    )
  )
