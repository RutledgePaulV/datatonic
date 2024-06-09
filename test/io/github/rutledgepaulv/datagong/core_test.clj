(ns io.github.rutledgepaulv.datagong.core-test
  (:require [clojure.test :refer :all]
            [io.github.rutledgepaulv.datagong.core :as dg]))

(def datoms
  [[1 :person/name "Paul"]
   [1 :person/age 32]
   [2 :person/name "David"]
   [2 :person/age 35]])

(def db (reduce dg/add-datom (dg/new-db) datoms))

(deftest index-routing
  (is (= {{:in #{:e :a} :out #{:v :e :a}}    #{:eav :aev}
          {:in #{:v :e} :out #{:v :e :a}}    #{:vea :eva}
          {:in #{:v :e :a} :out #{:v :e :a}} #{:vea :eav :eva :ave :aev :vae}
          {:in #{:e :a} :out #{}}            #{:ea :ae}
          {:in #{:v :e} :out #{:e}}          #{:ev :ve}
          {:in #{:v} :out #{}}               #{:v}
          {:in #{:e :a} :out #{:v}}          #{:eav :aev}
          {:in #{:v} :out #{:v :e}}          #{:ve}
          {:in #{:v} :out #{:v}}             #{:v}
          {:in #{:v} :out #{:e :a}}          #{:vea :vae}
          {:in #{:e :a} :out #{:e :a}}       #{:ea :ae}
          {:in #{} :out #{:v :e :a}}         #{:vea :eav :eva :ave :aev :vae}
          {:in #{} :out #{:v :e}}            #{:ev :ve}
          {:in #{:v :a} :out #{}}            #{:av :va}
          {:in #{:v :a} :out #{:e}}          #{:ave :vae}
          {:in #{:v} :out #{:v :e :a}}       #{:vea :vae}
          {:in #{:v :e :a} :out #{:e :a}}    #{:vea :eav :eva :ave :aev :vae}
          {:in #{} :out #{:v}}               #{:v}
          {:in #{:v :e} :out #{:v :a}}       #{:vea :eva}
          {:in #{:v :e :a} :out #{:v :e}}    #{:vea :eav :eva :ave :aev :vae}
          {:in #{:v :e :a} :out #{:v}}       #{:vea :eav :eva :ave :aev :vae}
          {:in #{:e} :out #{:v :e}}          #{:ev}
          {:in #{:e :a} :out #{:v :a}}       #{:eav :aev}
          {:in #{:e} :out #{:e}}             #{:e}
          {:in #{:a} :out #{:a}}             #{:a}
          {:in #{:v} :out #{:v :a}}          #{:va}
          {:in #{:v :a} :out #{:v :e}}       #{:ave :vae}
          {:in #{:v :e :a} :out #{:a}}       #{:vea :eav :eva :ave :aev :vae}
          {:in #{:e :a} :out #{:e}}          #{:ea :ae}
          {:in #{:a} :out #{}}               #{:a}
          {:in #{:v :a} :out #{:e :a}}       #{:ave :vae}
          {:in #{:v :e :a} :out #{:e}}       #{:vea :eav :eva :ave :aev :vae}
          {:in #{:v :e} :out #{:e :a}}       #{:vea :eva}
          {:in #{:v :a} :out #{:v}}          #{:av :va}
          {:in #{:a} :out #{:v :a}}          #{:av}
          {:in #{} :out #{:v :a}}            #{:av :va}
          {:in #{:e} :out #{:v}}             #{:ev}
          {:in #{} :out #{:a}}               #{:a}
          {:in #{:v :e :a} :out #{}}         #{:vea :eav :eva :ave :aev :vae}
          {:in #{:v :a} :out #{:v :a}}       #{:av :va}
          {:in #{:v :e} :out #{:a}}          #{:vea :eva}
          {:in #{:v} :out #{:a}}             #{:va}
          {:in #{:a} :out #{:e :a}}          #{:ae}
          {:in #{:v :a} :out #{:v :e :a}}    #{:ave :vae}
          {:in #{:e :a} :out #{:a}}          #{:ea :ae}
          {:in #{:v :e} :out #{:v :e}}       #{:ev :ve}
          {:in #{:e :a} :out #{:v :e}}       #{:eav :aev}
          {:in #{:v} :out #{:e}}             #{:ve}
          {:in #{:a} :out #{:e}}             #{:ae}
          {:in #{:a} :out #{:v}}             #{:av}
          {:in #{:e} :out #{:v :e :a}}       #{:eav :eva}
          {:in #{:e} :out #{:a}}             #{:ea}
          {:in #{} :out #{:e}}               #{:e}
          {:in #{:v :e} :out #{}}            #{:ev :ve}
          {:in #{} :out #{:e :a}}            #{:ea :ae}
          {:in #{:a} :out #{:v :e :a}}       #{:ave :aev}
          {:in #{:e} :out #{}}               #{:e}
          {:in #{:v :e :a} :out #{:v :a}}    #{:vea :eav :eva :ave :aev :vae}
          {:in #{:a} :out #{:v :e}}          #{:ave :aev}
          {:in #{:e} :out #{:v :a}}          #{:eav :eva}
          {:in #{:v :a} :out #{:a}}          #{:av :va}
          {:in #{:v :e} :out #{:v}}          #{:ev :ve}
          {:in #{:e} :out #{:e :a}}          #{:ea}}
         dg/index-router)))

(deftest basic-searches

  (testing "Correctly unifies two clauses on entity"
    (is (= '{:attrs  #{?age ?e}
             :tuples #{{?e 2 ?age 35}}}
           (dg/plan* db '[[?e :person/name "David"] [?e :person/age ?age]])))

    (is (= '{:attrs  #{?age ?e}
             :tuples #{{?e 1 ?age 32}}}
           (dg/plan* db '[[?e :person/name "Paul"] [?e :person/age ?age]]))))

  (testing "supports ORing two clauses instead of ANDing"
    (is (= '{:attrs  #{?e}
             :tuples #{{?e 1} {?e 2}}}
           (dg/plan* db '(or [?e :person/name "David"] [?e :person/name "Paul"])))))

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
