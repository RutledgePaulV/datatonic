(ns io.github.rutledgepaulv.datatonic.core
  (:require [io.github.rutledgepaulv.datatonic.plan :as plan]
            [io.github.rutledgepaulv.datatonic.execute :as execute]
            [io.github.rutledgepaulv.datatonic.aggregation :as aggregate]))

(def query-keywords
  #{:find :with :in :where})

(defn parse-query [query]
  (into {} (reduce
             (fn [parts form]
               (if (contains? query-keywords form)
                 (conj parts [form []])
                 (conj (pop parts) (conj (pop (peek parts))
                                         (conj (peek (peek parts)) form)))))
             [] query)))

(defn create-relation [in args]
  )

(defn q [query db & args]
  (let [{:keys [find in with where]} (parse-query query)
        initial-relation (create-relation in args)
        query-plan       (plan/plan db initial-relation where)
        query-result     (execute/execute* db query-plan)
        aggregation      (aggregate/aggregate* find with query-result)]
    aggregation))
