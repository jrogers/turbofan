(ns turbofan.jepsen-test
  "The client's side of the gateway's protocol, without a cluster."
  (:require [clojure.test :refer [deftest is]]
            [turbofan.jepsen :as t]))

(deftest a-transaction-is-one-line
  (is (= "a 1 5;r 1;r 2" (t/encode [[:append 1 5] [:r 1 nil] [:r 2 nil]]))))

(deftest answers-complete-the-op
  (let [write {:type :invoke, :f :txn, :value [[:append 1 5] [:r 1 nil]]}
        read  {:type :invoke, :f :txn, :value [[:r 1 nil]]}]
    (is (= {:type :ok, :f :txn, :value [[:append 1 5] [:r 1 [3 5]]]}
           (t/answer write "ok [[:append 1 5] [:r 1 [3 5]]]")))
    (is (= :fail (:type (t/answer write "fail list-full"))))
    (is (= :info (:type (t/answer write "info timeout"))) "a write may yet be applied")
    (is (= :info (:type (t/answer write nil))) "nor is a closed connection an answer")
    (is (= :fail (:type (t/answer read "info session-closed"))) "a read logs nothing")))
