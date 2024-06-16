(ns io.github.rutledgepaulv.datatonic.dyno
  "For optimizing a portion of a query plan during
   query execution (not during planning). This means
   the optimizer is allowed to interact with the data in
   the database to make decisions about what to execute
   (like cardinality).")


(defn dispatch [db rel plan]
  (first plan))

(defmulti optimize #'dispatch)

(defmethod optimize :default [db rel plan]
  plan)


(defn optimize* [db rel plan]
  (optimize db rel plan))
