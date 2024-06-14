(ns io.github.rutledgepaulv.datatonic.rules
  (:require [io.github.rutledgepaulv.lattice.core :as lattice]))


; build a graph of rule dependencies...
; if we can prove there's no recursion (cycle in the graph) then rules
; act essentially as a macro, and we can just do a static expansion and
; have performance equal to the raw terms.



; if the rules being used have recursion (cycles in the graph) then we
; need a custom evaluation strategy that pushes and pops stack frames


