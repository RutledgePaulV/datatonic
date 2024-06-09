(ns io.github.rutledgepaulv.datagong.core-test
  (:require [clojure.test :refer :all]
            [io.github.rutledgepaulv.datagong.core :as dg]))

(def datoms
  [[1 :person/name "Paul"]
   [1 :person/age 32]
   [2 :person/name "David"]
   [2 :person/age 35]])

(def db (reduce dg/add-datom (dg/new-db) datoms))


