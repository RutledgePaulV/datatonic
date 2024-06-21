(ns io.github.rutledgepaulv.datatonic.core-test
  (:require [clojure.test :refer :all])
  (:require [io.github.rutledgepaulv.datatonic.core :as d]
            [io.github.rutledgepaulv.datatonic.index :as index]))

(def datoms
  [[1 :person/name "Paul"]
   [1 :person/age 32]
   [2 :person/name "David"]
   [2 :person/age 35]])

(def db (reduce index/add-datom (index/new-db) datoms))

(deftest q-test
  (is (= '{:attrs  #{?age ?e ?name},
           :tuples #{{?e 1, ?name "Paul", ?age 32}}}
         (d/q
           '[:find ?name
             :in ?age
             :where
             [?e :person/age ?age]
             [?e :person/name ?name]]
           db
           32)))
  (is (= '{:attrs  #{?age ?e ?name},
           :tuples #{{?e 2, ?name "David", ?age 35}}}
         (d/q
           '[:find ?name
             :in ?age
             :where
             [?e :person/age ?age]
             [?e :person/name ?name]]
           db
           35))))
