(ns io.github.rutledgepaulv.datatonic.aggregation-test
  (:require [clojure.test :refer :all])
  (:require [io.github.rutledgepaulv.datatonic.aggregation :refer :all]))

(def relation
  '{:attrs  #{?e ?a ?v}
    :tuples #{{?e 1 ?a :person/name ?v "Paul"}
              {?e 2 ?a :person/name ?v "David"}}})

(deftest aggregation-test
  (is (= #{[1 :person/name "Paul"]
           [2 :person/name "David"]}
         (aggregate* '[?e ?a ?v] [] relation)))

  (is (= 1 (aggregate* '[?e .] [] relation)))

  (is (= [1 2] (aggregate* '[?e ...] [] relation)))

  (is (= #{[1 2]} (aggregate* '[(distinct ?v)] [] relation)))

  (is (= 1 (aggregate* '[(count ?a) .] '[] relation)))

  (is (= 2 (aggregate* '[(count ?a) .] '[?e] relation))))
