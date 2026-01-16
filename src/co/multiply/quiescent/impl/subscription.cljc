(ns ^:no-doc co.multiply.quiescent.impl.subscription
  #?(:cljs (:require-macros co.multiply.quiescent.impl.subscription))
  (:require
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.quiescent.type.call :as call]
    #?(:cljs [co.multiply.quiescent.impl.subscription.subscription :as sub])
    [co.multiply.quiescent.util :refer [if-cljs]])
  #?(:clj (:import
            [co.multiply.quiescent.impl.subscription
             Subscription TeardownSubscription CallbackSubscription
             UnsubSubscription CancelExecSubscription])))


(defmacro getPhase
  [this]
  (if-cljs
    `(co.multiply.quiescent.impl.subscription.subscription/getPhase ~this)
    `(Subscription/.getPhase ~this)))


(defmacro runSub
  [this task-state]
  (if-cljs
    `(co.multiply.quiescent.impl.subscription.subscription/runSub ~this ~task-state)
    `(Subscription/.runSub ~this ~task-state)))


(defn subscribe-teardown
  "Attach a subscription to task `task`, tearing down task `target` when reaching `phase-settling`.

   If `target` reaches `phase-settling` before `task`, remove the subscription from `task`."
  [task target]
  (cond
    ;; If `task` is already settling, just insert the teardown on it (will run immediately).
    (call/atOrPast task sm/phase-settling)
    (call/doSubscribe task
      #?(:clj  (TeardownSubscription. sm/phase-settling target)
         :cljs (sub/teardown-subscription sm/phase-settling target)))

    ;; If `target` is already settling, `task` can't possibly tear it down. No-op.
    (call/atOrPast target sm/phase-settling) nil

    ;; Neither task is settling yet. Subscribe ´this´ to tear down `target` if it settles first,
    ;; and `target` to remove the teardown subscription if it settles first.
    :else
    #?(:clj  (call/doSubscribe target
               (UnsubSubscription. sm/phase-settling task
                 ;; Subscribe `target` to be torn down when `task` settles.
                 (call/doSubscribe task
                   (TeardownSubscription. sm/phase-settling target))))
       :cljs (call/doSubscribe target
               (sub/unsub-subscription sm/phase-settling task
                 ;; Subscribe `target` to be torn down when `task` settles.
                 (call/doSubscribe task
                   (sub/teardown-subscription sm/phase-settling target)))))))


(defmacro subscribe-callback
  "Attach a subscription to `task`, running `f` when reaching `phase`

   `f` receives the current state of `this` as its only argument."
  [task phase f]
  `(call/doSubscribe ~task
     (if-cljs
       (co.multiply.quiescent.impl.subscription.subscription/callback-subscription ~phase ~f)
       (CallbackSubscription. ~phase ~f))))


(defmacro subscribe-cancel-exec
  "Attach a subscription to `task`, cancelling execution when reaching `phase`.

   In CLJ: `fut-or-fn` is a Future, cancelled via Future/.cancel
   In CLJS: `fut-or-fn` is a cancellation function that is called directly

   Only runs if the task was cancelled (checks TaskState.cancelled)."
  [task phase fut-or-fn]
  `(when-some [ffn# ~fut-or-fn]
     (call/doSubscribe ~task
       (if-cljs
         (co.multiply.quiescent.impl.subscription.subscription/cancel-exec-subscription ~phase ffn#)
         (CancelExecSubscription. ~phase ffn#)))))