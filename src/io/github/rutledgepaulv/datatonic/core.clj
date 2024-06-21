(ns io.github.rutledgepaulv.datatonic.core
  (:require [io.github.rutledgepaulv.datatonic.algebra :as algebra]
            [io.github.rutledgepaulv.datatonic.plan :as plan]
            [io.github.rutledgepaulv.datatonic.execute :as execute]
            [io.github.rutledgepaulv.datatonic.aggregation :as aggregate]
            [io.github.rutledgepaulv.datatonic.utils :as utils]))

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

(defn create-relation [templates values]
  {:attrs  (utils/logic-vars templates)
   :tuples (->> (algebra/** (map utils/destruct templates values))
                (remove empty?)
                (map (fn [xs] (apply merge xs))))})

(defn q [query db & args]
  (let [{:keys [find in with where]} (parse-query query)
        initial-relation (create-relation in args)
        query-plan       (plan/plan* db where (:attrs initial-relation #{}))
        query-result     (execute/execute* db query-plan initial-relation)
        aggregation      (aggregate/aggregate* find with query-result)]
    aggregation))

(defn plan [query db]
  (let [{:keys [find in with where]} (parse-query query)]
    (plan/plan* db where (utils/logic-vars in))))
