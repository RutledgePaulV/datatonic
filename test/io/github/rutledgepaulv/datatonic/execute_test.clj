(ns io.github.rutledgepaulv.datatonic.execute-test
  (:require [clojure.test :refer :all]
            [io.github.rutledgepaulv.datatonic.execute :as execute]
            [io.github.rutledgepaulv.datatonic.plan :as plan]
            [io.github.rutledgepaulv.datatonic.index :as index]))

(def datoms
  [[1 :person/name "Paul"]
   [1 :person/age 32]
   [2 :person/name "David"]
   [2 :person/age 35]])

(def datascript-indices
  [[:e :a :v]
   [:a :e :v]
   [:a :v :e]])

(def datomic-indices
  [[:e :a :v]
   [:a :e :v]
   [:a :v :e]
   [:v :a :e]])

(def recommended-indices
  [[:e :a :v]
   [:a :e :v]
   [:a :v :e]
   [:v :a :e]
   [:a]])

(def all-indices (reduce index/add-datom (index/new-db) datoms))
(def datomic-esq (reduce index/add-datom (index/new-db [:e :a :v] datomic-indices) datoms))
(def datascript-esq (reduce index/add-datom (index/new-db [:e :a :v] datascript-indices) datoms))
(def recommended (reduce index/add-datom (index/new-db [:e :a :v] recommended-indices) datoms))
(def dbs [all-indices datomic-esq datascript-esq recommended])

(defn plan
  ([q]
   (plan q all-indices))
  ([q db]
   (plan/plan* db q)))

(defn execute
  ([q]
   (execute q all-indices))
  ([q db]
   (execute/execute* db (plan/plan* db q))))


(deftest basic-searches

  (testing "predicates"
    (are [query expected] (apply = expected (map (partial execute query) dbs))
      '[[?e]
        [(even? ?e)]]
      '{:attrs  #{?e}
        :tuples #{{?e 2}}}

      '[[?e ?a ?v]
        [(even? ?e)]
        (and
          [(number? ?v)]
          [(odd? ?v)])]
      '{:attrs  #{?a ?v ?e}
        :tuples #{{?e 2 ?a :person/age ?v 35}}}))

  (testing "not removes the elements which unify"
    (are [query expected] (apply = expected (map (partial execute query) dbs))
      '[[?e :person/age ?a]
        (not [?e :person/name "Paul"])]
      '{:attrs  #{?a ?e}
        :tuples #{{?e 2 ?a 35}}}

      '[[?e :person/age ?a]
        (not [?e :person/name "Paul"])
        (not [?e :person/age 32])]
      '{:attrs  #{?a ?e}
        :tuples #{{?e 2 ?a 35}}}))

  (testing "Correctly unifies two clauses on entity"
    (are [query expected] (apply = expected (map (partial execute query) dbs))
      '[[?e :person/name "David"]
        [?e :person/age ?age]]
      '{:attrs  #{?age ?e}
        :tuples #{{?e 2 ?age 35}}}

      '[[?e :person/name "Paul"]
        [?e :person/age ?age]]
      '{:attrs  #{?age ?e}
        :tuples #{{?e 1 ?age 32}}}))

  (testing "supports ORing two clauses instead of ANDing"
    (are [query expected] (apply = expected (map (partial execute query) dbs))
      '(or [?e :person/name "David"]
           [?e :person/name "Paul"])
      '{:attrs  #{?e}
        :tuples #{{?e 1} {?e 2}}}))

  (testing "supports partial (prefix) matches"
    (are [query expected] (apply = expected (map (partial execute query) dbs))
      '[[?e :person/name]]
      '{:attrs  #{?e}
        :tuples #{{?e 1} {?e 2}}}))

  (testing "supports introducing synthetic bindings"
    (are [query expected] (apply = expected (map (partial execute query) dbs))
      '[[?e :person/name]
        [(even? ?e) ?even]]
      '{:attrs  #{?e ?even}
        :tuples #{{?even false ?e 1}
                  {?even true ?e 2}}}))

  (testing "supports non-recursive rules"
    (are [query rules expected]
         (->> dbs
              (map (fn [db] (assoc db :rules rules)))
              (map (partial execute query))
              (apply = expected))
      '[(names ?name)]
      '[[(names ?name)
         [_ :person/name ?name]]
        [(names ?name)
         [?e :person/age ?age]
         [?e :person/name ?name]]]
      '{:attrs  #{?name}
        :tuples #{{?name "Paul"}
                  {?name "David"}}}))

  (testing "supports recursive rules"
    (let [db    (reduce index/add-datom (index/new-db)
                        [[1 :person/father 2]
                         [1 :person/name "Paul"]
                         [2 :person/father 3]
                         [2 :person/name "James"]
                         [3 :person/name "Paul"]])
          rules '[[(paternals ?child ?ancestor)
                   [?child :person/father ?ancestor]]
                  [(paternals ?child ?ancestor)
                   [?child :person/father ?ancestor-1]
                   (paternals ?ancestor-1 ?ancestor)]]
          db'   (assoc db :rules rules)]
      (is (= '{:attrs  #{?paternal ?p}
               :tuples #{{?paternal 3, ?p 1} {?p 1, ?paternal 2}}}
             (execute '[[?p :person/name "Paul"]
                        (paternals ?p ?paternal)]
                      db')))

      (is (= '{:attrs  #{?paternal ?p}
               :tuples #{{?p 1, ?paternal 2}}}
             (execute '[[?p :person/name "Paul"]
                        (paternals ?p ?paternal)
                        [?paternal :person/name "James"]]
                      db')))))

  (testing "supports all free variable matches"
    (are [query expected] (apply = expected (map (partial execute query) dbs))
      '[[?e]]
      '{:attrs  #{?e}
        :tuples #{{?e 1} {?e 2}}}

      '[[?e ?a]]
      '{:attrs  #{?a ?e}
        :tuples #{{?e 2 ?a :person/age}
                  {?e 1 ?a :person/age}
                  {?e 2 ?a :person/name}
                  {?e 1 ?a :person/name}}}

      '[[?e ?a ?v]]
      '{:attrs  #{?a ?v ?e}
        :tuples #{{?e 1 ?a :person/name ?v "Paul"}
                  {?e 2 ?a :person/age ?v 35}
                  {?e 2 ?a :person/name ?v "David"}
                  {?e 1 ?a :person/age ?v 32}}})))
