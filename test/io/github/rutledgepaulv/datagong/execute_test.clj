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

(def db (reduce index/add-datom (index/new-db) datoms))

(defn execute [term]
  (let [plan (plan/plan* db term)]
    (execute/execute* db plan)))

(deftest basic-searches

  (testing "Correctly unifies two clauses on entity"
    (is (= '{:attrs  #{?age ?e}
             :tuples #{{?e 2 ?age 35}}}
           (execute '[[?e :person/name "David"] [?e :person/age ?age]])))

    (is (= '{:attrs  #{?age ?e}
             :tuples #{{?e 1 ?age 32}}}
           (execute '[[?e :person/name "Paul"] [?e :person/age ?age]]))))

  (testing "supports ORing two clauses instead of ANDing"
    (is (= '{:attrs  #{?e}
             :tuples #{{?e 1} {?e 2}}}
           (execute '(or [?e :person/name "David"] [?e :person/name "Paul"])))))

  (testing "supports partial (prefix) matches"
    (is (= '{:attrs  #{?e}
             :tuples #{{?e 1} {?e 2}}}
           (execute '[[?e :person/name]]))))

  (testing "supports all free variable matches"

    (is (= '{:attrs  #{?e}
             :tuples #{{?e 1} {?e 2}}}
           (execute '[[?e]])))

    (is (= '{:attrs  #{?a ?e}
             :tuples #{{?e 2 ?a :person/age}
                       {?e 1 ?a :person/age}
                       {?e 2 ?a :person/name}
                       {?e 1 ?a :person/name}}}
           (execute '[[?e ?a]])))

    (is (= '{:attrs  #{?a ?v ?e}
             :tuples #{{?e 1 ?a :person/name ?v "Paul"}
                       {?e 2 ?a :person/age ?v 35}
                       {?e 2 ?a :person/name ?v "David"}
                       {?e 1 ?a :person/age ?v 32}}}
           (execute '[[?e ?a ?v]])))))
