(ns ^:no-doc co.multiply.quiescent.impl.subscription.subscription
  (:require
    [co.multiply.quiescent.type :refer [TaskState]]
    [co.multiply.quiescent.type.call :as call]))


(defprotocol ISubscription
  (getPhase [this])
  (runSub [this task-state]))


(deftype TeardownSubscription [^:mutable prev ^:mutable next ^:mutable deque phase target]
  Object
  (getNext [_this] next)
  (casNext [_this new-next] (set! next new-next) true)
  (getPrev [_this] prev)
  (setPrev [_this new-prev] (set! prev new-prev))
  (getDeque [_this] deque)
  (casDeque [_this expected new-val]
    (if (= deque expected)
      (do (set! deque new-val) true)
      false))

  ISubscription
  (getPhase [_this] phase)
  (runSub [_this _state]
    (call/doCancelCascade target)))


(defn teardown-subscription
  [phase target]
  (TeardownSubscription. nil nil nil phase target))


(deftype CallbackSubscription [^:mutable prev ^:mutable next ^:mutable deque phase f]
  Object
  (getNext [_this] next)
  (casNext [_this new-next] (set! next new-next) true)
  (getPrev [_this] prev)
  (setPrev [_this new-prev] (set! prev new-prev))
  (getDeque [_this] deque)
  (casDeque [_this expected new-val]
    (if (= deque expected)
      (do (set! deque new-val) true)
      false))

  ISubscription
  (getPhase [_this] phase)
  (runSub [_this ^TaskState state]
    (try (f state)
      (catch js/Error e
        (throw (ex-info "Subscription threw during task lifecycle." {:error e}))))))


(defn callback-subscription
  [phase f]
  (CallbackSubscription. nil nil nil phase f))


(deftype UnsubSubscription [^:mutable prev ^:mutable next ^:mutable deque phase target sub]
  Object
  (getNext [_this] next)
  (casNext [_this new-next] (set! next new-next) true)
  (getPrev [_this] prev)
  (setPrev [_this new-prev] (set! prev new-prev))
  (getDeque [_this] deque)
  (casDeque [_this expected new-val]
    (if (= deque expected)
      (do (set! deque new-val) true)
      false))

  ISubscription
  (getPhase [_this] phase)
  (runSub [_this _state]
    (call/doUnsubscribe target sub)))


(defn unsub-subscription
  [phase target sub]
  (UnsubSubscription. nil nil nil phase target sub))


(deftype CancelExecSubscription [^:mutable prev ^:mutable next ^:mutable deque phase cancel-fn]
  Object
  (getNext [_this] next)
  (casNext [_this new-next] (set! next new-next) true)
  (getPrev [_this] prev)
  (setPrev [_this new-prev] (set! prev new-prev))
  (getDeque [_this] deque)
  (casDeque [_this expected new-val]
    (if (= deque expected)
      (do (set! deque new-val) true)
      false))

  ISubscription
  (getPhase [_this] phase)
  (runSub [_this ^TaskState state]
    (when (.-cancelled state)
      (cancel-fn))))


(defn cancel-exec-subscription
  [phase cancel-fn]
  (CancelExecSubscription. nil nil nil phase cancel-fn))
