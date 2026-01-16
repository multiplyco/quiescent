(ns co.multiply.quiescent.binding-test
  (:require
    [clojure.test :refer [deftest is testing #?(:cljs async)]]
    [co.multiply.quiescent :as q :refer [qdo]]
    [co.multiply.quiescent.test-support :refer [allow-platform-park make-exception with-task]]
    [co.multiply.scoped :refer [ask scoping]]))


(allow-platform-park)

(def ^:dynamic *test-binding*)
(def ^:dynamic *outer-binding*)
(def ^:dynamic *inner-binding*)


(defn create-scope-cascade
  []
  (q/task
    [(ask *outer-binding*)
     (scoping [*outer-binding* 1
               *inner-binding* 2]
       [(ask *outer-binding*)
        (q/task (ask *outer-binding*))
        (ask *inner-binding*)
        (q/task (ask *inner-binding*))
        (scoping [*inner-binding* 3]
          {:inner (ask *inner-binding*) :outer (ask *outer-binding*)})
        (scoping [*inner-binding* 4]
          #{(ask *inner-binding*) (ask *outer-binding*)})
        (q/qlet [a (q/task (ask *inner-binding*))
                 b (q/task (+ a 1))]
          [(ask *outer-binding*) a b])
        (q/qfor [i [5 6]]
          (q/task (+ i (ask *inner-binding*))))
        (q/qmerge {:x (q/task (ask *outer-binding*))
                   :y (q/task (ask *inner-binding*))})])]))


(defn setup-chain
  [kind base handler-fn]
  (testing (str "Scoped values propagate through with " kind)
    (let [expected-chain-result [0 [1 1 2 2 {:inner 3 :outer 1} #{1 4} [1 2 3] [7 8] {:x 1 :y 2}]]]
      #?(:clj  (let [res (promise)]
                 (scoping [*outer-binding* 0]
                   (handler-fn base
                     (fn [& _]
                       (q/compel
                         (-> (create-scope-cascade)
                           (q/then (partial deliver res)))))))
                 (is (= expected-chain-result @res)))
         :cljs (async done
                 (let [res (atom nil)]
                   (scoping [*outer-binding* 0]
                     (handler-fn base
                       (fn [& _]
                         (q/compel
                           (-> (create-scope-cascade)
                             (q/then (partial reset! res))
                             (q/finally
                               (fn [& _]
                                 (is (= expected-chain-result @res))
                                 (js/queueMicrotask done))))))))))))))


(deftest scope-then
  (setup-chain "`then`" (q/task :done) q/then)
  #?(:clj (setup-chain "`then-cpu`" (q/cpu-task :done) q/then-cpu)))


(deftest scope-then-multi
  (setup-chain "`then` (multi)" (q/task :done) (partial q/then (q/task :done)))
  #?(:clj (setup-chain "`then-cpu` (multi)" (q/cpu-task :done) (partial q/then-cpu (q/cpu-task :done)))))


(deftest scope-ok
  (setup-chain "`ok`" (q/task :done) q/ok)
  #?(:clj (setup-chain "`ok-cpu`" (q/cpu-task :done) q/ok-cpu)))


(deftest scope-handle
  (setup-chain "`handle`" (q/task :done) q/handle)
  #?(:clj (setup-chain "`handle-cpu`" (q/cpu-task :done) q/handle-cpu)))


(deftest scope-catch
  (setup-chain "`catch`" (q/task (throw (make-exception "test"))) q/catch)
  #?(:clj (setup-chain "`catch-cpu`" (q/cpu-task (throw (make-exception "test"))) q/catch-cpu)))


(deftest scope-err
  (setup-chain "`err`" (q/task (throw (make-exception "test"))) q/err)
  #?(:clj (setup-chain "`err-cpu`" (q/cpu-task (throw (make-exception "test"))) q/err-cpu)))


(deftest scope-finally
  (setup-chain "`finally`" (q/task :done) q/finally)
  #?(:clj (setup-chain "`finally-cpu`" (q/cpu-task :done) q/finally-cpu)))


(deftest scope-finally-error
  (setup-chain "`finally` (error)" (q/task (throw (make-exception "test"))) q/finally)
  #?(:clj (setup-chain "`finally-cpu` (error)" (q/cpu-task (throw (make-exception "test"))) q/finally-cpu)))


(deftest scope-finally-cancelled
  (setup-chain "`finally` (cancelled)" (doto (q/sleep 10000) q/cancel) q/finally)
  #?(:clj (setup-chain "`finally-cpu` (cancelled)" (doto (q/sleep 10000) q/cancel) q/finally-cpu)))


(deftest scope-done
  (setup-chain "`done`" (q/task :done) q/done)
  #?(:clj (setup-chain "`done-cpu`" (q/cpu-task :done) q/done-cpu)))


(deftest scope-done-error
  (setup-chain "`done` (error)" (q/task (throw (make-exception "test"))) q/done)
  #?(:clj (setup-chain "`done-cpu` (error)" (q/cpu-task (throw (make-exception "test"))) q/done-cpu)))


(deftest individual-chain-scopes
  (testing "Multiple subscriptions each see their own scoped values"
    (let [obs1 (atom nil)
          obs2 (atom nil)
          obs3 (atom nil)
          task (q/task :result)]
      (with-task (scoping [*test-binding* :outer]
                   (qdo
                     (scoping [*test-binding* :first]
                       (q/ok task (fn [_] (reset! obs1 (ask *test-binding*)))))
                     (scoping [*test-binding* :second]
                       (q/ok task (fn [_] (reset! obs2 (ask *test-binding*)))))
                     (reset! obs3 (ask *test-binding*))))
        (fn [_v _e _c]
          (is (= :first @obs1))
          (is (= :second @obs2))
          (is (= :outer @obs3)))))))
