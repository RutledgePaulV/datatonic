(ns io.github.rutledgepaulv.datagong.execute-test
  (:require [clojure.test :refer :all]
            [io.github.rutledgepaulv.datagong.execute :as execute]
            [io.github.rutledgepaulv.datagong.plan :as plan]
            [io.github.rutledgepaulv.datagong.index :as index]))

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

(def all-indices (reduce index/add-datom (index/new-db) datoms))
(def datomic-esq (reduce index/add-datom (index/new-db [:e :a :v] datomic-indices) datoms))
(def datascript-esq (reduce index/add-datom (index/new-db [:e :a :v] datascript-indices) datoms))

(def dbs [all-indices datomic-esq datascript-esq])

(defn execute [term db]
  (->> term
       (plan/plan* db)
       (execute/execute* db)))

(deftest basic-searches

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
      '{:attrs  #{?e ?even},
        :tuples #{{?even false, ?e 1}
                  {?even true, ?e 2}}}))

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
