(ns co.multiply.quiescent.test-support
  #?(:cljs (:require-macros co.multiply.quiescent.test-support))
  (:require
    [clojure.test :refer [use-fixtures deftest testing]]
    [co.multiply.machine-latch :as ml]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.impl.executor :as executor]
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.quiescent.impl.subscription :as subs]
    [co.multiply.quiescent.type :as type #?@(:cljs [:refer [TaskState]])]
    [co.multiply.quiescent.type.call :as call]
    [co.multiply.quiescent.util :refer [if-cljs]]
    [co.multiply.scoped :refer [with-scope scoping]])
  #?(:clj (:import [co.multiply.quiescent.impl TaskState])))


#?(:clj (defn platform-thread-fixture
          "Fixture that allows dereferencing Tasks on platform threads.
           Tests run on platform threads, but Quiescent's MachineLatch
           normally throws when awaited from non-virtual threads.

           Use with: (use-fixtures :once platform-thread-fixture)"
          [f]
          (q/throw-on-platform-park! false)
          (try (f)
            (finally
              (q/throw-on-platform-park! true)))))


(defmacro allow-platform-park
  []
  (if-cljs
    nil
    `(use-fixtures :once platform-thread-fixture)))


#_(co.multiply.quiescent.test-support/allow-platform-park)


(defmacro with-task
  [task f]
  (if-cljs
    `(clojure.test/async done#
       (q/finally (type/as-task ~task) (fn [v# e# c#] (~f v# e# c#) (js/queueMicrotask done#))))
    `(scoping [ml/*assert-virtual* false]
       (let [t# (type/as-task ~task)]
         @(q/await t#)
         (if (call/isExceptional t#)
           (~f nil (call/getResult t#) (call/isCancelled t#))
           (~f (call/getResult t#) nil (call/isCancelled t#)))))))


(defmacro def-results
  [nm & definitions]
  `(do ~@(for [[_clause addendum description with] definitions]
           `(deftest ~(symbol (str nm "-" (name addendum)))
              ~(if (string? description)
                 `(testing ~description ~with)
                 description)))))


(defn clause
  ([addendum with]
   [addendum with])
  ([addendum description with]
   [addendum description with]))


(defmacro result
  [bind & body]
  `(fn ~(conj bind '& '_)
     ~@body))


(defn expect
  "Return a task that awaits the given taskable. Returns true regardless of outcome.

   Impervious to cancellation of the taskable, and does not propagate cancellation to observed tasks."
  [t f]
  (let [task     (type/as-task t)
        scope    (call/getScope task)
        observer (impl/-pending-task executor/delegate-virtual)]
    (subs/subscribe-callback task sm/phase-quiescent
      (fn [^TaskState task-state]
        (doto observer
          (call/doRun
            (fn []
              (with-scope scope
                (try
                  (if (.-exceptional task-state)
                    (f nil (.-result task-state) (.-cancelled task-state))
                    (f (.-result task-state) nil false))
                  (catch #?(:clj Throwable :cljs :default) e
                    (call/doApply observer nil e)))))))))
    (subs/subscribe-teardown observer task)
    observer))


(defn make-exception
  [msg]
  #?(:clj  (Exception. ^String msg)
     :cljs (js/Error. msg)))
