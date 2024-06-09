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
  (is (= '{:attrs #{?age ?e}, :tuples #{{?e 2, ?age 35}}}
         (:relation
           (dg/plan* db '[[?e :person/name "David"]
                          [?e :person/age ?age]]))))

  (is (= '{:attrs #{?age ?e}, :tuples #{{?e 1, ?age 32}}}
         (:relation
           (dg/plan* db '[[?e :person/name "Paul"]
                          [?e :person/age ?age]])))))
