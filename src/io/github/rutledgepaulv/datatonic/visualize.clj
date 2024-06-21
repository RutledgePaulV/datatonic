(ns io.github.rutledgepaulv.datatonic.visualize
  (:require [clojure.java.io :as io]
            [io.github.rutledgepaulv.datatonic.aggregation :as aggregate]
            [io.github.rutledgepaulv.datatonic.execute :as execute]
            [io.github.rutledgepaulv.datatonic.index :as index]
            [io.github.rutledgepaulv.datatonic.plan :as plan]
            [io.github.rutledgepaulv.datatonic.core :as d]
            [clojure.java.browse :as browse]
            [hiccup2.core :as h]
            [rhizome.viz :as rhi])
  (:import (java.util Base64)))


(def ^:dynamic *log* (atom []))

(defn instrument-plan [db bindings node]
  (with-meta (plan/plan db bindings node) {:raw node}))

(defn instrument-execute [db relation node]
  (swap! *log* conj {:node        node
                     :predecessor (:node (peek (deref *log*)))
                     :relation    relation})
  (let [new-relation (execute/execute db relation node)]
    (swap! *log* conj {:node        node
                       :predecessor (:node (peek (deref *log*)))
                       :relation    new-relation})
    new-relation))

(defn graphviz-html [x]
  (str "<" x ">"))

(defn render-table
  ([relation]
   (render-table relation 5))
  ([relation max-size]
   (when-not (empty? (:attrs relation))
     (graphviz-html
       (h/html
         (let [headers (sort (:attrs relation))]
           [:table
            [:tr (for [header (cons "#" headers)] [:td header])]
            (for [[idx row] (map vector (range) (take max-size (:tuples relation)))]
              [:tr (cons
                     [:td idx]
                     (for [head headers]
                       [:td (pr-str (get row head))]))])
            (when (< max-size (count (:tuples relation)))
              [:tr (cons
                     [:td (str (dec max-size) "-" (dec (count (:tuples relation))))]
                     (for [_ headers] [:td "..."]))])]))))))

(defn visual [log]
  (let [nodes
        (into #{} (remove nil?) (mapcat identity (for [{:keys [node predecessor]} log] [node predecessor])))
        g
        (reduce (fn [g {:keys [node predecessor]}]
                  (if (not= node predecessor)
                    (update g predecessor (fnil conj []) node)
                    g))
                {} log)]
    (->
      (rhizome.dot/graph->dot
        nodes
        g
        :options {:node {:fontname ""}
                  :edge {:fontname ""}}
        :vertical? true
        :node->descriptor (fn [node] {:shape    "box"
                                      :label    (pr-str (:raw (meta node)))})
        :edge->descriptor (fn [source target]
                            (reduce
                              (fn [nf it]
                                (if (and (= (:node it) target) (= (:predecessor it) source))
                                  (reduced {:label    (render-table (:relation it))})
                                  nf))
                              nil
                              log)))
      (clojure.string/replace "dpi=100, " "")
      (clojure.string/replace "\"<<" "<<")
      (clojure.string/replace ">>\"" ">>"))))


(defn dot* [db query & args]
  (binding [*log*           (atom [])
            plan/*recur*    #'instrument-plan
            execute/*recur* #'instrument-execute]
    (let [{:keys [find in with where]} (d/parse-query query)
          initial-relation (d/create-relation in args)
          query-plan       (plan/plan* db where (:attrs initial-relation #{}))
          query-result     (execute/execute* db query-plan initial-relation)
          aggregation      (aggregate/aggregate* find with query-result)]
      (visual
        (cons
          {:node (with-meta {:begin true} {:raw 'query}) :relation initial-relation}
          (-> (update (deref *log*) 0 (fn [x] (assoc x :predecessor {:begin true})))
              (conj {:node        (with-meta {:begin true} {:raw 'query})
                     :predecessor (:node (peek (deref *log*)))
                     :relation    (:relation (peek (deref *log*)))})))))))

(defn visualize* [db query & args]
  (let [dot  (apply dot* db query args)
        svg  (rhi/dot->svg dot)
        file (io/file (str (random-uuid) ".svg"))]
    (spit file svg)
    (#'browse/open-url-in-browser (str "file://" (.getAbsolutePath file)))))

(comment

  (def datoms
    [[1 :person/name "Paul"]
     [1 :person/age 32]
     [2 :person/name "David"]
     [2 :person/age 35]])

  (def db (reduce index/add-datom (index/new-db) datoms))

  (visualize*
    db
    '[:find ?e ?a ?v
      :where
      [?e ?a ?v]
      [(even? ?e)]
      [(number? ?v) ?num]])

  )
