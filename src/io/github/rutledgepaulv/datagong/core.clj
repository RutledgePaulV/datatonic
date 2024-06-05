(ns io.github.rutledgepaulv.datagong.core
  (:require [me.tonsky.persistent-sorted-set :as pss]))

(def components
  {:e {:name "entity" :index 0}
   :a {:name "attribute" :index 1}
   :v {:name "value" :index 2}})

(def indexes
  {:eav {:order [:e :a :v]}
   :eva {:order [:e :v :a]}
   :aev {:order [:a :e :v]}
   :ave {:order [:a :v :e]}
   :vae {:order [:v :a :e]}
   :ae  {:order [:a :e]}
   :av  {:order [:a :v]}
   :ev  {:order [:e :v]}
   :ve  {:order [:v :e]}
   :va  {:order [:v :a]}
   :e   {:order [:e]}
   :a   {:order [:a]}
   :v   {:order [:v]}})

(defn new-db
  ([] (new-db compare))
  ([comparator]
   (persistent!
     (reduce-kv
       (fn [agg k v]
         (assoc! agg k (pss/sorted-set-by comparator)))
       (transient {})
       indexes))))

(defn add-datom [db datom]
  (persistent!
    (reduce-kv
      (fn [agg k v]
        (let [{:keys [order]} (get indexes k)]
          (assoc! agg k
                  (conj v
                        (persistent!
                          (reduce (fn [v component]
                                    (conj! v (nth datom (get-in components [component :index]))))
                                  (transient [])
                                  order))))))
      (transient {})
      db)))

(def index-selection
  {{:in #{} :out #{:a}}               {:plan {:index :a :operation :scan}}
   {:in #{} :out #{:e}}               {:plan {:index :e :operation :scan}}
   {:in #{} :out #{:v}}               {:plan {:index :v :operation :scan}}

   {:in #{} :out #{:a :e}}            {:plan {:index :ae :operation :scan}}
   {:in #{} :out #{:a :v}}            {:plan {:index :av :operation :scan}}
   {:in #{} :out #{:e :v}}            {:plan {:index :ev :operation :scan}}
   {:in #{} :out #{:a :e :v}}         {:plan {:index :eav :operation :scan}}

   {:in #{:e} :out #{:e}}             {:plan {:index :e :operation :range :start [:e] :stop [:e]}}
   {:in #{:a} :out #{:a}}             {:plan {:index :a :operation :range :start [:a] :stop [:a]}}
   {:in #{:v} :out #{:v}}             {:plan {:index :v :operation :range :start [:v] :stop [:v]}}

   {:in #{:e} :out #{:e :a}}          {:plan {:index :ea :operation :range :start [:e] :stop [:e]}}
   {:in #{:e} :out #{:e :v}}          {:plan {:index :ev :operation :range :start [:e] :stop [:e]}}
   {:in #{:a} :out #{:a :e}}          {:plan {:index :ae :operation :range :start [:a] :stop [:a]}}
   {:in #{:a} :out #{:a :v}}          {:plan {:index :av :operation :range :start [:a] :stop [:a]}}
   {:in #{:v} :out #{:a :v}}          {:plan {:index :va :operation :range :start [:v] :stop [:v]}}
   {:in #{:v} :out #{:e :v}}          {:plan {:index :ve :operation :range :start [:v] :stop [:v]}}

   {:in #{:e} :out #{:e :a :v}}       {:plan {:index :eav :operation :range :start [:e] :stop [:e]}}
   {:in #{:a} :out #{:a :e :v}}       {:plan {:index :aev :operation :range :start [:a] :stop [:a]}}
   {:in #{:v} :out #{:a :e :v}}       {:plan {:index :vae :operation :range :start [:v] :stop [:v]}}

   {:in #{:e :v} :out #{:v :e}}       {:plan {:index :ev :operation :range :start [:e :v] :stop [:e :v]}}
   {:in #{:a :e} :out #{:e :a}}       {:plan {:index :ea :operation :range :start [:e :a] :stop [:e :a]}}
   {:in #{:a :v} :out #{:v :a}}       {:plan {:index :av :operation :range :start [:a :v] :stop [:a :v]}}

   {:in #{:e :v} :out #{:v :e :a}}    {:plan {:index :eva :operation :range :start [:e :v] :stop [:e :v]}}
   {:in #{:a :e} :out #{:v :e :a}}    {:plan {:index :eav :operation :range :start [:e :a] :stop [:e :a]}}
   {:in #{:a :v} :out #{:v :e :a}}    {:plan {:index :ave :operation :range :start [:a :v] :stop [:a :v]}}

   {:in #{:a :e :v} :out #{:v :e :a}} {:plan {:index :eav :operation :range :start [:e :a :v] :stop [:e :a :v]}}})

(defn logic-var? [x]
  (and (symbol? x) (clojure.string/starts-with? (name x) "?")))

(defn get-logic-vars [[e a v]]
  (cond-> {}
    (logic-var? e)
    (assoc :e e)
    (logic-var? a)
    (assoc :a a)
    (logic-var? v)
    (assoc :v v)))

(defn reverse-map [x]
  (into {} (map (comp rseq vec)) x))

(defn pattern-clause? [x]
  (and (vector? x) (not (seq? (first x)))))

(defn binding-clause? [x]
  (and (vector? x) (= 2 (count x)) (seq? (first x))))

(defn predicate-clause? [x]
  (and (vector? x) (= 1 (count x)) (seq? (first x))))

(defn rule? [x]
  (and (seq? x) (symbol? (first x))))

(defn dispatch [ctx clause]
  (cond
    (pattern-clause? clause)
    :pattern
    (binding-clause? clause)
    :binding
    (predicate-clause? clause)
    :predicate
    (rule? clause)
    (if (contains? #{'or 'or-join 'not-join 'and 'and-join 'not} (first clause))
      (keyword (first clause))
      :rule)))

(defmulti plan #'dispatch)

(defmethod plan :default [ctx clause]
  (throw (ex-info "Unsupported query clause." {:clause clause})))

(defmethod plan :pattern [{:keys [relations] :as ctx} clause]
  (let [logic-vars (get-logic-vars clause)]
    (println logic-vars)
    ctx))

(defmethod plan :predicate [{:keys [relations] :as ctx} clause]
  ctx)

(defmethod plan :binding [{:keys [relations] :as ctx} clause]
  ctx)

(defmethod plan :and [context [_ & children :as clause]]
  (reduce
    (fn [ctx' child]
      (plan ctx' child))
    context
    children))

(defmethod plan :or [ctx [_ & children :as clause]]
  (reduce
    (fn [ctx' child]
      (plan ctx' child))
    ctx
    children))

(defmethod plan :not [ctx [_ & children :as clause]]
  ctx)

(defmethod plan :or-join [ctx [_ & children :as clause]]
  (reduce
    (fn [ctx' child]
      (plan ctx' child))
    ctx
    children))

(defmethod plan :and-join [ctx [_ & children :as clause]]
  (reduce
    (fn [ctx' child]
      (plan ctx' child))
    ctx
    children))

(defmethod plan :not-join [ctx [_ & children :as clause]]
  (reduce
    (fn [ctx' child]
      (plan ctx' child))
    ctx
    children))

(defn plan* [clauses]
  (plan {} (cons 'and clauses)))

(defn execute* [db plan]
  )


(comment

  (def query
    '[:find ?e ?n
      :where
      [?e :person/name ?n]
      [?e :person/age ?a]
      [(> ?a 21)]])

  )
