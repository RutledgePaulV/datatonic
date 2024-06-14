(ns io.github.rutledgepaulv.datatonic.plan
  (:require [clojure.set :as sets]
            [clojure.string :as str]
            [io.github.rutledgepaulv.datatonic.utils :as utils]
            [io.github.rutledgepaulv.datatonic.index :as index]))

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
       {:index (keyword (str/join (map name selected-index)))
        :in    (merge const-bindings (utils/reverse-map (select-keys reversed bound-logic-vars)))
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

  (plan* (index/new-db) '(or-join [?e]
                                  (and [?e :person/name "David"]
                                       [?e :person/age 35])
                                  (and [?e :person/name "Paul"]
                                       [?e :person/age 32])))

  #_[:or {:in #{}, :out #{?e}}
     [:and {:in #{}, :out #{?e}}
      [:search {:index :vae, :in {:a :person/name, :v "David"}, :out {:e ?e}}]
      [:search {:index :vea, :in {:a :person/age, :v 35, :e ?e}, :out {:e ?e}}]]
     [:and {:in #{}, :out #{?e}}
      [:search {:index :vae, :in {:a :person/name, :v "Paul"}, :out {:e ?e}}]
      [:search {:index :vea, :in {:a :person/age, :v 32, :e ?e}, :out {:e ?e}}]]]

  (plan* (index/new-db) '(and-join [?e]
                                   (and [?e :person/name "David"]
                                        [?e :person/age 35])
                                   (and [?e :person/name "Paul"]
                                        [?e :person/age 32])))

  #_[:and {:in #{}, :out #{?e}}
     [:and {:in #{}, :out #{?e}}
      [:search {:index :vae, :in {:a :person/name, :v "David"}, :out {:e ?e}}]
      [:search {:index :vea, :in {:a :person/age, :v 35, :e ?e}, :out {:e ?e}}]]
     [:and {:in #{?e}, :out #{?e}}
      [:search {:index :vea, :in {:a :person/name, :v "Paul", :e ?e}, :out {:e ?e}}]
      [:search {:index :vea, :in {:a :person/age, :v 32, :e ?e}, :out {:e ?e}}]]]

  (plan* (index/new-db) '[[?e :person/name ?name] (not [?e :person/age 35])])

  #_[:and {:in #{}, :out #{?e ?name}}
     [:search {:index :ave, :in {:a :person/name}, :out {:e ?e, :v ?name}}]
     [:not {:in #{?e}, :out #{}}
      [:search {:index :vea, :in {:a :person/age, :v 35, :e ?e}, :out {:e ?e}}]]]

  )
