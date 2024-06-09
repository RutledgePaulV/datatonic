(ns io.github.rutledgepaulv.datagong.core-test
  (:require [clojure.test :refer :all]
            [io.github.rutledgepaulv.datagong.core :as dg]))

(def datoms
  [[1 :person/name "Paul"]
   [1 :person/age 32]
   [2 :person/name "David"]
   [2 :person/age 35]])

(def db (reduce dg/add-datom (dg/new-db) datoms))


(deftest basic-searches

  (testing "Correctly unifies two clauses on entity"
    (is (= '{:attrs  #{?age ?e}
             :tuples #{{?e 2 ?age 35}}}
           (dg/plan* db '[[?e :person/name "David"]
                          [?e :person/age ?age]])))
    (is (= '{:attrs  #{?age ?e}
             :tuples #{{?e 1 ?age 32}}}
           (dg/plan* db '[[?e :person/name "Paul"]
                          [?e :person/age ?age]]))))

  (testing "supports ORing two clauses instead of ANDing"
    (is (= '{:attrs  #{?e}
             :tuples #{{?e 1} {?e 2}}}
           (dg/plan* db '(or
                           [?e :person/name "David"]
                           [?e :person/name "Paul"])))))

  (testing "supports partial (prefix) matches"
    (is (= '{:attrs  #{?e}
             :tuples #{{?e 1} {?e 2}}}
           (dg/plan* db '[[?e :person/name]]))))

  (testing "supports all free variable matches"

    (is (= '{:attrs  #{?e}
             :tuples #{{?e 1} {?e 2}}}
           (dg/plan* db '[[?e]])))

    (is (= '{:attrs  #{?a ?e}
             :tuples #{{?e 2 ?a :person/age}
                       {?e 1 ?a :person/age}
                       {?e 2 ?a :person/name}
                       {?e 1 ?a :person/name}}}
           (dg/plan* db '[[?e ?a]])))

    (is (= '{:attrs  #{?a ?v ?e}
             :tuples #{{?e 1 ?a :person/name ?v "Paul"}
                       {?e 2 ?a :person/age ?v 35}
                       {?e 2 ?a :person/name ?v "David"}
                       {?e 1 ?a :person/age ?v 32}}}
           (dg/plan* db '[[?e ?a ?v]])))))
