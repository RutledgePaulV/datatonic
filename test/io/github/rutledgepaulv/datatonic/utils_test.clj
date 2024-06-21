(ns io.github.rutledgepaulv.datatonic.utils-test
  (:require [clojure.test :refer :all])
  (:require [io.github.rutledgepaulv.datatonic.utils :refer [destruct]]))


(deftest destruct-test
  (is (= '#{{?a 1}} (destruct '?a 1)))
  (is (= '#{{?a 1 ?b 3}} (destruct '[?a ?b] [1 3])))
  (is (= '#{{?a [1 3]}} (destruct '?a [1 3])))
  (is (= '#{{?a 1} {?a 3}} (destruct '[?a ...] [1 3])))
  (is (= '#{{?a 1, ?b 2} {?a 3, ?b 4}} (destruct '[[?a ?b] ...] [[1 2] [3 4]]))))
