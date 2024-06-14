(ns io.github.rutledgepaulv.datatonic.static-optimizer
  "Given a database and a query plan, rewrite the plan
  to apply static optimizations. The static optimizer
  may insert dynamic optimization aspect nodes if it
  notices something might benefit from dynamic optimization."
  (:require [clojure.walk :as walk]))

(defn static-dispatch [db plan]
  (first plan))

(defmulti static-optimize #'static-dispatch)

(defmethod static-optimize :default [db plan] plan)


; AND NODES CAN BE FREELY REORDERED (don't forget to adjust the in/out props along the way)
; THE GOAL IS TO NARROW THE DATASET AS FAST AS POSSIBLE, SO YOU WANT TO PRIORITIZE GOOD INDEX
; USAGE EARLIER IF USING LESS THAN A COMPLETE SET OF INDICES. DYNAMICALLY YOU WANT TO PRIORITIZE
; FEWER DATOMS EARLIER (requires cardinality estimation in the general case)
(defmethod static-optimize :and [db plan]
  plan
  )

(defn make-data-inaccessible
  "Make data inaccessible so nobody implements a dynamic optimization step
   as part of the static optimization phase. Instead, optimizers should
   statically add a dynamic optimization node to the plan."
  [db]
  (walk/postwalk (fn [form] (if (map? form) (dissoc form :data) form)) db))

(defn static-optimize* [db plan]
  (static-optimize (make-data-inaccessible db) plan))
