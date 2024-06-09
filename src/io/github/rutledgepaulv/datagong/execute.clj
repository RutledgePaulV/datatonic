(ns io.github.rutledgepaulv.datagong.execute
  (:require [io.github.rutledgepaulv.datagong.utils :as utils]))


(defmulti execute (fn [db relation node] (first node)))


(defmethod execute :and [db relation [_ & children]]
  )

(defmethod execute :or [db relation [_ & children]]
  )

(defmethod execute :not [db relation [_ & children]]
  )

(defmethod execute :and-join [db relation [_ & children]]
  )

(defmethod execute :or-join [db relation [_ & children]]
  )

(defmethod execute :not-join [db relation [_ & children]]
  )

(defmethod execute :search [db relation [_ & children]]
  )

(defmethod execute :binding [db relation [_ & children]]
  )

(defmethod execute :predicate [db relation [_ & children]]
  )


