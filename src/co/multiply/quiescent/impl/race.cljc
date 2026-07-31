(ns ^:no-doc co.multiply.quiescent.impl.race
  (:require
    [co.multiply.quiescent.impl :as impl]
    [co.multiply.quiescent.impl.executor :refer [delegate-virtual delegate-sync]]
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.conc.integer :as integer :refer [new-integer]]
    [co.multiply.conc.set :as cset]
    [co.multiply.quiescent.impl.subscription :as subs]
    [co.multiply.quiescent.type :as type :refer [#?(:cljs TaskState)]]
    [co.multiply.quiescent.type.call :as call]
    [co.multiply.scoped :refer [with-scope]])
  #?(:clj (:import [co.multiply.quiescent.impl ITask TaskState])))


(defn race
  "Race multiple tasks, returning the first successful result.

   When multiple tasks complete, the first non-exceptional result wins.
   Losing tasks are cancelled via cascade cancellation.

   Options (second arity):
   - `:tf`            - Transform function applied to winner's result
   - `:release`       - Cleanup function for stateful resources from losing tasks
                        (only called for non-nil results that weren't the winner)
   - `:cancel-losers?` - When true (default), the race owns its entrants: losers
                         are cancelled when the winner settles. When false, the
                         race merely observes: losers keep running, and no
                         cancellation ever propagates from the returned task.

   Edge cases:
   - Empty tasks: Returns a task containing nil
   - Single task: Returns that task directly (owning mode only)
   - All fail: Returns task with exception (single error or ex-info with :errors key)
   - All cancelled: Returns cancelled task

   Note: Duplicate task instances are handled correctly - only one result
   is tracked per unique value."
  ([tasks]
   (race tasks nil))
  ([tasks {:keys [tf release cancel-losers?] :or {cancel-losers? true}}]
   (let [tasks (mapv type/as-task tasks)]
     (cond
       ;; Racing nothing returns nil.
       (empty? tasks)
       (impl/do-applier delegate-sync nil)

       ;; In a race with one task, the task given wins (or throws). Only valid
       ;; when owning: handing back the entrant itself would let callers cancel
       ;; it through the result. Observing races fall through to the general
       ;; case, which wraps the entrant.
       (and cancel-losers? (= 1 (count tasks)))
       (first tasks)

       :else
       (let [winner     (impl/-pending-task delegate-sync)
             task-latch (new-integer (type/vecCount tasks))
             results    (cset/new-set)
             errors     (cset/new-set)]
         (doseq [t tasks]
           ;; Register participating tasks to be torn down when `winner` settles.
           ;; Skipped when observing: entrants outlive the race untouched.
           (when cancel-losers?
             (subs/subscribe-teardown winner t))
           ;; Set up the race.
           (subs/subscribe-callback t sm/phase-settling
             (fn handle-race-participant
               [^TaskState state]
               (if (.-exceptional state)
                 ;; If an exception occurred, collect it.
                 (do (cset/add errors (.-result state))
                   ;; For every exception, count down. If we reach zero, all tasks
                   ;; yielded exceptions, and we must fail the race.
                   (when (zero? (integer/decrementAndGet task-latch))
                     (if (every? impl/cancelled? tasks)
                       (call/doCancel winner "Race cancelled due to all participants cancelled.")
                       (let [all-errors (vec errors)]
                         (call/doApply winner nil
                           (if (= 1 (count all-errors))
                             ;; Handle the edge case of racing one failing task.
                             (first all-errors)
                             ;; When there are multiple exceptions, return all,
                             ;; wrapped in an ex-info.
                             (ex-info "All tasks failed." {:errors all-errors})))))))
                 ;; Value was successful.
                 (let [result (.-result state)]
                   ;; Handle releasing of stateful resources if a `release` function is supplied.
                   ;; Only non-nil results are supported due to constraints of KeySetView.
                   (if (and release (some? result))
                     ;; If this is the first time we see the value, proceed. No point in going
                     ;; further otherwise.
                     (when (cset/add results result)
                       ;; Attempt to apply the result to `winner`.
                       (when-not (call/doApply winner result nil tf)
                         ;; If application failed, run cleanup on the value: it's a realized loser.
                         (delegate-virtual
                           (fn teardown-stateful-non-winner
                             []
                             (with-scope (call/getScope t)
                               (release result))))))
                     ;; Otherwise, attempt to win without cleanup.
                     (call/doApply winner result nil tf)))))))
         winner)))))
