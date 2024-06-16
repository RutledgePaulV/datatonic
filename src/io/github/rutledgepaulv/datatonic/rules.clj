(ns io.github.rutledgepaulv.datatonic.rules
  (:require [io.github.rutledgepaulv.lattice.core :as lattice]
            [io.github.rutledgepaulv.lattice.protocols :as lp]))


; build a graph of rule dependencies...
; if we can prove there's no recursion (cycle in the graph) then rules
; are essentially just as a macro, and we can just do a static expansion
; and have performance equal to the raw terms.



; if the rules being used have recursion (cycles in the graph) then we
; need a custom evaluation strategy that pushes and pops stack frames



(defn create-rule-graph [rules]
  (reduce
    (fn [agg [[rule-name & bindings] & clauses]]
      (let [result     {:rule-name rule-name
                        :bindings  bindings
                        :clauses   clauses}
            depends-on (into #{} (map (fn [x] [(first x) (count (rest x))]) (filter seq? clauses)))]
        (cond-> agg
          :always
          (update-in [:nodes [rule-name (count bindings)]] (fnil conj #{}) result)
          (not-empty depends-on)
          (update-in [:graph [rule-name (count bindings)]] (fnil conj #{}) depends-on))))
    {:nodes {} :graph {}}
    rules))

(defn rewrite-rule-invocation [rule-index rule]
  )


(comment

  (def rules
    '[[(ancestor ?c ?p)
       [?p _ ?c]]
      [(ancestor ?c ?p)
       [?p1 _ ?c]
       (ancestor ?p1 ?p)]])

  )
