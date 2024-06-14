(ns io.github.rutledgepaulv.datatonic.optimize
  "Given a database and a query plan, rewrite the
   plan to apply optimizations. Optimizations can
   be static or dynamic (injection of a dynamic
   optimization step into the plan which will be
   carried about the executor)."
  (:require [clojure.walk :as walk]))

(defn dispatch [db node]
  (first node))

(defmulti optimize #'dispatch)

(defmethod optimize :default [db node] node)




(defn make-data-inaccessible
  "Make data inaccessible so nobody implements a dynamic optimization step
   as part of the static optimization phase. Instead, optimizers should
   statically add a dynamic optimization node to the plan."
  [db]
  (walk/postwalk (fn [form] (if (map? form) (dissoc form :data) form)) db))

(defn optimize* [db plan]
  (optimize (make-data-inaccessible db) plan))
