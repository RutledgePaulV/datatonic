(ns io.github.rutledgepaulv.datatonic.dynamic-optimizer
  "Given a database and a relation and a query plan,
  rewrite the plan. Dynamic optimizations are those
  which may look at and calculate database statistics
  as part of making decisions.")

(defn dispatch [db relation plan]
  (first plan))

(defmulti optimize #'dispatch)

(defmethod optimize :default [db relation plan]
  plan)

(defn optimize* [db relation plan]
  (optimize db relation plan))

