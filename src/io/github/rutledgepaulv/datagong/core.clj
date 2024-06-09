(ns io.github.rutledgepaulv.datagong.core
  (:require [clojure.set :as sets]
            [me.tonsky.persistent-sorted-set :as pss]
            [io.github.rutledgepaulv.datagong.algebra :as algebra]))

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

(defn safe-compare [a b]
  (try
    (compare a b)
    (catch Exception e
      (compare
        [(str (class a)) (hash a)]
        [(str (class b)) (hash b)]))))

(defn get-position [order component]
  (case order
    [:a] (case component :a 0)
    [:e] (case component :e 0)
    [:v] (case component :v 0)
    [:a :e] (case component :e 0 :a 1)
    [:a :v] (case component :a 0 :v 1)
    [:e :v] (case component :e 0 :v 1)
    [:v :a] (case component :a 0 :v 1)
    [:v :e] (case component :e 0 :v 1)
    [:a :e :v] (case component :e 0 :a 1 :v 2)
    [:a :v :e] (case component :e 0 :a 1 :v 2)
    [:e :a :v] (case component :e 0 :a 1 :v 2)
    [:e :v :a] (case component :e 0 :a 1 :v 2)
    [:v :a :e] (case component :e 0 :a 1 :v 2)))

(defn create-comparator [order]
  (fn [a b]
    (reduce
      (fn [result element]
        (let [result'
              (let [a' (nth a (get-position order element))
                    b' (nth b (get-position order element))]
                (safe-compare a' b'))]
          (if (zero? result') result (reduced result'))))
      0
      order)))

(defn new-db
  ([] (new-db create-comparator))
  ([create-comparator]
   (persistent!
     (reduce-kv
       (fn [agg k {:keys [order]}]
         (assoc! agg k (pss/sorted-set-by (create-comparator order))))
       (transient {})
       indexes))))

(defn add-datom [db datom]
  (persistent!
    (reduce-kv
      (fn [agg k v]
        (let [{:keys [order]} (get indexes k)]
          (assoc! agg k (conj v
                              (let [components (set order)]
                                (cond-> []
                                  (contains? components :e)
                                  (conj (nth datom 0))
                                  (contains? components :a)
                                  (conj (nth datom 1))
                                  (contains? components :v)
                                  (conj (nth datom 2))))))))
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

(defn get-constants [[e a v]]
  (cond-> {}
    (and (some? e) (not (logic-var? e)))
    (assoc :e e)
    (and (some? a) (not (logic-var? a)))
    (assoc :a a)
    (and (some? v) (not (logic-var? v)))
    (assoc :v v)))

(defn reverse-map [x]
  (into {} (map (comp vec rseq vec)) x))

(defn pattern-clause? [x]
  (and (vector? x) (not (seq? (first x)))))

(defn binding-clause? [x]
  (and (vector? x) (= 2 (count x)) (seq? (first x))))

(defn predicate-clause? [x]
  (and (vector? x) (= 1 (count x)) (seq? (first x))))

(defn rule? [x]
  (and (seq? x) (symbol? (first x))))

(defn execute-search [db plan inputs outputs]
  {:attrs  (set (vals outputs))
   :tuples (for [tuple (if (= :range (:operation plan))
                         (subseq (get-in db [(:index plan)]) (get inputs (:start plan)) (get inputs (:stop plan)))
                         (get-in db [(:index plan)]))]
             (reduce
               (fn [agg [k v]]
                 (assoc agg v (case k :e (nth tuple 0)
                                      :a (nth tuple 1)
                                      :v (nth tuple 2))))
               {} outputs))})

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

(defmethod plan :pattern [{:keys [relation db] :as ctx} clause]
  (let [logic-vars (get-logic-vars clause)
        rev-logic  (reverse-map logic-vars)
        const-vars (get-constants clause)
        const-keys (set (keys const-vars))
        logic-keys (set (keys logic-vars))
        new-rel    (cond
                     ; pattern is at least partially constrained by existing relation
                     (algebra/intersects? (:attrs relation) (set (vals logic-vars)))
                     (reduce (fn [relation binding]
                               (let [binding-vars     (into {} (map (fn [[logic-var-name value]]
                                                                      {(get rev-logic logic-var-name) value}))
                                                            binding)

                                     known-positions  (sets/union (set (keys binding-vars)) const-keys)
                                     output-positions (set (keys logic-vars))
                                     index-plan       (get-in index-selection [{:in known-positions :out output-positions} :plan])
                                     search-relation  (execute-search db index-plan (merge binding-vars const-vars) logic-vars)]
                                 (algebra/join relation search-relation)))
                             relation
                             (:tuples (algebra/projection relation logic-vars)))

                     :else
                     (let [index-plan      (get-in index-selection [{:in const-keys :out logic-keys} :plan])
                           search-relation (execute-search db index-plan const-vars logic-vars)]
                       (algebra/union relation search-relation)))]
    (assoc ctx :relation new-rel)))

(defmethod plan :predicate [{:keys [relation] :as ctx} clause]
  ctx)

(defmethod plan :binding [{:keys [relation] :as ctx} clause]
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

(defmethod plan :rule [ctx clause]
  )


(defn plan* [db clauses]
  (plan {:relation (algebra/create-relation) :db db} (cons 'and clauses)))

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
