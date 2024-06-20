(ns io.github.rutledgepaulv.datatonic.plan-test
  (:require [clojure.test :refer :all]
            [io.github.rutledgepaulv.datatonic.index :as index]
            [io.github.rutledgepaulv.datatonic.plan :as plan]))

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

(defn plan
  ([q]
   (plan q all-indices))
  ([q db]
   (plan/plan* db q)))


(deftest basic-searches

  (testing "predicates"
    (are [db query expected] (= expected (plan query db))
      all-indices
      '[[?e]
        [(even? ?e)]]
      '[:and {:in #{}, :out #{?e}}
        [:search {:index :e, :in {}, :out {:e ?e}}]
        [:predicate {:in #{?e}, :args [?e], :fn clojure.core/even?, :out #{}}]]

      all-indices
      '[[?e ?a ?v]
        [(even? ?e)]
        (and
          [(number? ?v)]
          [(odd? ?v)])]
      '[:and {:in #{}, :out #{?a ?v ?e}}
        [:search {:index :vea, :in {}, :out {:e ?e, :a ?a, :v ?v}}]
        [:predicate {:in #{?e}, :args [?e], :fn clojure.core/even?, :out #{}}]
        [:and {:in #{?a ?v ?e}, :out #{?a ?v ?e}}
         [:predicate {:in #{?v}, :args [?v], :fn clojure.core/number?, :out #{}}]
         [:predicate {:in #{?v}, :args [?v], :fn clojure.core/odd?, :out #{}}]]]))

  (testing "not removes the elements which unify"
    (are [db query expected] (= expected (plan query db))
      all-indices
      '[[?e :person/age ?a]
        (not [?e :person/name "Paul"])]
      '[:and {:in #{}, :out #{?a ?e}}
        [:search {:index :ave, :in {:a :person/age}, :out {:e ?e, :v ?a}}]
        [:not {:in #{?e}, :out #{?e}}
         [:search {:index :vea, :in {:a :person/name, :v "Paul", :e ?e}, :out {:e ?e}}]]]

      all-indices
      '[[?e :person/age ?a]
        (not [?e :person/name "Paul"])
        (not [?e :person/age 32])]
      '[:and {:in #{}, :out #{?a ?e}}
        [:search {:index :ave, :in {:a :person/age}, :out {:e ?e, :v ?a}}]
        [:not {:in #{?e}, :out #{?e}}
         [:search {:index :vea, :in {:a :person/name, :v "Paul", :e ?e}, :out {:e ?e}}]]
        [:not {:in #{?e}, :out #{?e}}
         [:search {:index :vea, :in {:a :person/age, :v 32, :e ?e}, :out {:e ?e}}]]]))

  (testing "Correctly unifies two clauses on entity"
    (are [db query expected] (= expected (plan query db))

      all-indices
      '[[?e :person/name "David"]
        [?e :person/age ?age]]
      '[:and {:in #{}, :out #{?age ?e}}
        [:search {:index :vae, :in {:a :person/name, :v "David"}, :out {:e ?e}}]
        [:search {:index :eav, :in {:a :person/age, :e ?e}, :out {:e ?e, :v ?age}}]]

      all-indices
      '[[?e :person/name "Paul"]
        [?e :person/age ?age]]
      '[:and {:in #{}, :out #{?age ?e}}
        [:search {:index :vae, :in {:a :person/name, :v "Paul"}, :out {:e ?e}}]
        [:search {:index :eav, :in {:a :person/age, :e ?e}, :out {:e ?e, :v ?age}}]]))

  (testing "supports ORing two clauses instead of ANDing"
    (are [db query expected] (= expected (plan query db))

      all-indices
      '(or [?e :person/name "David"]
           [?e :person/name "Paul"])
      '[:or {:in #{}, :out #{?e}}
        [:search {:index :vae, :in {:a :person/name, :v "David"}, :out {:e ?e}}]
        [:search {:index :vae, :in {:a :person/name, :v "Paul"}, :out {:e ?e}}]]))

  (testing "supports partial (prefix) matches"
    (are [db query expected] (= expected (plan query db))

      all-indices
      '[[?e :person/name]]
      '[:and {:in #{}, :out #{?e}}
        [:search {:index :ae, :in {:a :person/name}, :out {:e ?e}}]]))

  (testing "supports introducing synthetic bindings"
    (are [db query expected] (= expected (plan query db))

      all-indices
      '[[?e :person/name]
        [(even? ?e) ?even]]
      '[:and {:in #{}, :out #{?e ?even}}
        [:search {:index :ae, :in {:a :person/name}, :out {:e ?e}}]
        [:binding {:in #{?e}, :fn clojure.core/even?, :args [?e], :out #{?even}, :out-pattern ?even}]]))

  (testing "supports all free variable matches"
    (are [db query expected] (= expected (plan query db))

      all-indices
      '[[?e]]
      '[:and {:in #{}, :out #{?e}}
        [:search {:index :e, :in {}, :out {:e ?e}}]]

      all-indices
      '[[?e ?a]]
      '[:and {:in #{}, :out #{?a ?e}}
        [:search {:index :ea, :in {}, :out {:e ?e, :a ?a}}]]

      all-indices
      '[[?e ?a ?v]]
      '[:and {:in #{}, :out #{?a ?v ?e}}
        [:search {:index :vea, :in {}, :out {:e ?e, :a ?a, :v ?v}}]])))
