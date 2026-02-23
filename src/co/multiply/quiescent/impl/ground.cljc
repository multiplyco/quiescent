(ns ^:no-doc co.multiply.quiescent.impl.ground
  (:require
    [co.multiply.pathling :as p]
    [co.multiply.quiescent.impl.state-machine :as sm]
    [co.multiply.conc.integer :as integer :refer [new-integer]]
    [co.multiply.quiescent.impl.subscription :as subs]
    [co.multiply.pathling.accumulator :refer [acc-size acc-get acc-set!]]
    [co.multiply.quiescent.type :as type :refer [plain? plain-val #?(:cljs TaskState)]])
  #?(:clj (:import
            [co.multiply.quiescent.impl TaskState]
            [java.util HashMap])))


(defn ground
  "Resolve nested tasks in a value, replacing them with their results.

   Walks `v` looking for values that satisfy `groundable?` (Tasks, CompletableFutures,
   etc.). Found tasks are awaited in parallel. When all complete, they're replaced
   with their dereferenced values in the original data structure.

   Optimized by task count:
   - 0 tasks: Return immediately (no coordination overhead)
   - 1 task: Single subscription (minimal overhead)
   - N tasks: AtomicInteger countdown + object-array (full coordination)

   Callback signature:
   - Success: (on-complete grounded-value nil)
   - Failure: (on-complete nil exceptional-task-state)

   CAS semantics is handled by `on-complete`."
  [this v on-complete]
  (if (plain? v)
    (on-complete (plain-val v) nil)
    (let [{:keys [matches nav]} (p/path-when v type/groundable? {:raw-matches true})
          task-count (or (some-> matches (acc-size)) 0)]
      (case (unchecked-int task-count)
        ;; The result contained no inner tasks; return as is.
        0 (on-complete v nil)
        ;; There's exactly one inner task. Hand over execution to it via a subscription.
        1 (let [inner-task (type/as-task (acc-get matches 0))]
            ;; When this task enters the settling phase for any reason, cancel the inner task.
            (subs/subscribe-teardown this inner-task)
            ;; When the inner task produces a value, insert it on the data structure
            ;; in place of the task and call on-complete.
            (subs/subscribe-callback inner-task sm/phase-settling
              (fn inner-task-completion
                [^TaskState inner-task-state]
                (if (.-exceptional inner-task-state)
                  (on-complete nil inner-task-state)
                  (on-complete
                    (p/update-paths v nav (acc-set! matches 0 (.-result inner-task-state)))
                    nil)))))
        ;; There are many inner tasks. Await them concurrently and then hand over execution
        ;; to the one that completes last.
        (let [task-done-count (new-integer task-count)]
          (loop [idx (unchecked-int 0)]
            (when (< idx task-count)
              (let [inner-task (type/as-task (acc-get matches idx))]
                ;; When this task enters the settling phase for any reason, cancel the inner task.
                ;; References are held weakly so that they can be GC'd when tasks finish at different
                ;; times.
                (subs/subscribe-teardown this inner-task)
                ;; Runs when a value is available for the inner task.
                (subs/subscribe-callback inner-task sm/phase-settling
                  (fn resolve-inner-task
                    [^TaskState inner-task-state]
                    (if (.-exceptional inner-task-state)
                      ;; An exception leads to immediate termination of ground, and the
                      ;; subsequent cancellation of all sibling inner tasks.
                      (on-complete nil inner-task-state)
                      (do (acc-set! matches idx (.-result inner-task-state))
                        (when (zero? (integer/decrementAndGet task-done-count))
                          ;; When all inner tasks have produced values, replace them within the
                          ;; data structure with their resolved values.
                          (on-complete
                            (p/update-paths v nav matches)
                            nil)))))))
              (recur (unchecked-inc-int idx)))))))))
