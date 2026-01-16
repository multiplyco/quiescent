(ns ^:no-doc co.multiply.quiescent.type.call
  #?(:cljs (:require-macros co.multiply.quiescent.type.call))
  (:require
    [co.multiply.quiescent.util :refer [if-cljs]]
    [co.multiply.quiescent.type :as type #?@(:cljs [:refer [ITask IStateful TaskState]])])
  #?(:clj (:import [co.multiply.quiescent.impl ITask IStateful TaskState ICancellable])))


(defmacro isExceptional
  [this]
  (if-cljs
    `(type/isExceptional ~this)
    `(IStateful/.isExceptional ~this)))


(defmacro isCancelled
  [this]
  (if-cljs
    `(type/isCancelled ~this)
    `(IStateful/.isCancelled ~this)))


(defmacro isCompelled
  [this]
  (if-cljs
    `(type/isCompelled ~this)
    `(IStateful/.isCompelled ~this)))


(defmacro getPhase
  [this]
  (if-cljs
    `(type/getPhase ~this)
    `(IStateful/.getPhase ~this)))


(defmacro getResult
  [this]
  (if-cljs
    `(type/getResult ~this)
    `(IStateful/.getResult ~this)))


(defmacro getSubscriptions
  [this]
  (if-cljs
    `(type/getSubscriptions ~this)
    `(IStateful/.getSubscriptions ~this)))


(defmacro getScope
  [this]
  (if-cljs
    `(type/getScope ~this)
    `(IStateful/.getScope ~this)))


(defmacro setScope
  [this new-scope]
  (if-cljs
    `(type/setScope ~this ~new-scope)
    `(IStateful/.setScope ~this ~new-scope)))


(defmacro getNow
  [this default]
  (if-cljs
    `(type/getNow ~this ~default)
    `(ITask/.getNow ~this ~default)))


(defmacro newQuiescenceProxy
  ([this]
   (if-cljs
     `(type/newQuiescenceProxy ~this true)
     `(ITask/.newQuiescenceProxy ~this true)))
  ([this default]
   (if-cljs
     `(type/newQuiescenceProxy ~this ~default)
     `(ITask/.newQuiescenceProxy ~this ~default))))


(defmacro atOrPast
  [this phase]
  (if-cljs
    `(type/atOrPast ~this ~phase)
    `(IStateful/.atOrPast ~this ~phase)))


(defmacro doSubscribe
  [this sub]
  (if-cljs
    `(type/doSubscribe ~this ~sub)
    `(ITask/.doSubscribe ~this ~sub)))


(defmacro doCancel
  [this msg]
  (if-cljs
    `(type/doCancel ~this ~msg)
    `(ICancellable/.doCancel ~this ^String ~msg)))


(defmacro doCancelCascade
  [this]
  (if-cljs
    `(type/doCancelCascade ~this)
    `(ICancellable/.doCancelCascade ~this)))


(defmacro doCancelDirect
  [this]
  (if-cljs
    `(type/doCancelDirect ~this)
    `(ICancellable/.doCancelDirect ~this)))


(defmacro doUnsubscribe
  [this sub]
  (if-cljs
    `(type/doUnsubscribe ~this ~sub)
    `(ITask/.doUnsubscribe ~this ~sub)))


(defmacro doRun
  ([this f]
   (if-cljs
     `(type/doRun ~this ~f nil)
     `(ITask/.doRun ~this ~f nil)))
  ([this f tf]
   (if-cljs
     `(type/doRun ~this ~f ~tf)
     `(ITask/.doRun ~this ~f ~tf))))


(defmacro runSubscriptions
  [this]
  (if-cljs
    `(type/runSubscriptions ~this)
    `(ITask/.runSubscriptions ~this)))


(defmacro doGround
  ([this f]
   (if-cljs
     `(type/doGround ~this ~f nil)
     `(ITask/.doGround ~this ~f nil)))
  ([this f tf]
   (if-cljs
     `(type/doGround ~this ~f ~tf)
     `(ITask/.doGround ~this ~f ~tf))))


(defmacro doApply
  ([this v]
   `(co.multiply.quiescent.type.call/doGround ~this ~v))
  ([this v e]
   `(let [e# ~e]
      (if e#
        (co.multiply.quiescent.type.call/doWrite ~this
          (if-cljs
            (type/TaskState. true false e#)
            (TaskState. true false e#)))
        (co.multiply.quiescent.type.call/doGround ~this ~v))))
  ([this v e tf]
   `(let [e# ~e]
      (if e#
        (co.multiply.quiescent.type.call/doWrite ~this
          (if-cljs
            (type/TaskState. true false e#)
            (TaskState. true false e#)))
        (co.multiply.quiescent.type.call/doGround ~this ~v ~tf)))))


(defmacro doWrite
  [this task-state]
  (if-cljs
    `(type/doWrite ~this ~task-state)
    `(ITask/.doWrite ~this ~task-state)))


(defmacro doTransform
  [this res tf]
  (if-cljs
    `(type/doTransform ~this ~res ~tf)
    `(ITask/.doTransform ~this ~res ~tf)))


(defmacro doSettle
  [this]
  (if-cljs
    `(type/doSettle ~this)
    `(ITask/.doSettle ~this)))


(defmacro doQuiesce
  [this]
  (if-cljs
    `(type/doQuiesce ~this)
    `(ITask/.doQuiesce ~this)))


(defmacro doThen
  [this executor f]
  (if-cljs
    `(type/doThen ~this ~executor ~f)
    `(ITask/.doThen ~this ~executor ~f)))


(defmacro doCatch
  [this executor f]
  (if-cljs
    `(type/doCatch ~this ~executor ~f)
    `(ITask/.doCatch ~this ~executor ~f)))


(defmacro doCatchTyped
  [this executor f]
  (if-cljs
    `(type/doCatchTyped ~this ~executor ~f)
    `(ITask/.doCatchTyped ~this ~executor ~f)))


(defmacro doHandle
  [this executor f]
  (if-cljs
    `(type/doHandle ~this ~executor ~f)
    `(ITask/.doHandle ~this ~executor ~f)))


(defmacro doOk
  [this executor f]
  (if-cljs
    `(type/doOk ~this ~executor ~f)
    `(ITask/.doOk ~this ~executor ~f)))


(defmacro doErr
  [this executor f]
  (if-cljs
    `(type/doErr ~this ~executor ~f)
    `(ITask/.doErr ~this ~executor ~f)))


(defmacro doDone
  [this executor f]
  (if-cljs
    `(type/doDone ~this ~executor ~f)
    `(ITask/.doDone ~this ~executor ~f)))


(defmacro doFinally
  [this executor f]
  (if-cljs
    `(type/doFinally ~this ~executor ~f)
    `(ITask/.doFinally ~this ~executor ~f)))
