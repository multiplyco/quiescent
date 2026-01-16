(ns ^:no-doc co.multiply.quiescent.impl.state-machine
  (:require
    [co.multiply.machine-latch :as ml]))


(def phase-pending
  "Task created, not yet executing"
  :pending)


(def phase-running
  "Body executing on executor"
  :running)


(def phase-grounding
  "Body done, resolving nested tasks (ground)"
  :grounding)


(def phase-transforming
  "Applying transform function to grounded value."
  :transforming)


(def phase-writing
  "Writing final result to task state. All completion paths converge here."
  :writing)


(def phase-settling
  "Result available, running `then` callbacks and cascade cancel.

   When a task reaches settling:
   - Its result is available (deref returns)
   - Grounded children are *at least* settling (we observed their settling)
   - Cascade cancellation is attempted on registered children (best effort)

   We do NOT guarantee children are quiescent - only that they've started winding down.
   This is intentional: strong lifecycle coupling prevents GC, risks deadlocks, and
   doesn't match real-world async (you can't force a thread to stop immediately)."
  :settling)


(def phase-quiescent
  "Lifecycle complete. Parent has done its work; children may still be settling.

   Quiescent means this task's lifecycle is finished, not that the entire subtree
   has stopped. Registered children received cancel signals (if alive and not compelled),
   orphaned tasks are unknown (may be GC'd or still running)."
  :quiescent)


(def action-run
  "Transition pending → running. Task body begins execution."
  :run)


(def action-ground
  "Transition to grounding. Entry point for grounding:
   - running → grounding (after executing function)
   - pending → grounding (direct value via doApply)"
  :ground)


(def action-transform
  "Transition grounding → transforming. Applies transform function to grounded value."
  :transform)


(def action-write
  "Transition to writing. Writes final result to state. Entry from:
   - pending/running (cancellation or exception)
   - grounding (no transform)
   - transforming (after transform)"
  :write)


(def action-settle
  "Transition writing → settling. Run settling subscriptions."
  :settle)


(def action-quiesce
  "Transition settling → quiescent. Teardown complete."
  :quiesce)


(def ^:private state-machine
  "Task lifecycle state machine (7 phases, 6 actions).

   Phases progress linearly in the happy path:
     pending → running → grounding → transforming → writing → settling → quiescent

   But cancellation or error can jump from early phases directly to writing:
     pending ────────────────────────────────────────╮
     running ───────────────────────────────────────╮│
     grounding ────────────────────────────────────╮││
     transforming ────────────────────────────────╮│││
                                                  ↓↓↓↓
                                               writing → settling → quiescent

   All completion paths (success, error, cancel) converge at writing.

   Phase boundary guarantees: When a thread reaches phase B, all subscriptions
   for phase A have been fired (though not necessarily completed—we don't wait).
   This is enforced by MachineLatch CAS: only one thread wins each transition.

   Subscriptions run synchronously on the transitioning thread, providing
   predictable ordering without spawning overhead."
  {:states      [phase-pending phase-running phase-grounding phase-transforming phase-writing phase-settling phase-quiescent]
   :transitions {action-run       {phase-pending phase-running}
                 action-ground    {phase-running phase-grounding
                                   phase-pending phase-grounding}
                 action-transform {phase-grounding phase-transforming}
                 action-write     {phase-pending      phase-writing
                                   phase-running      phase-writing
                                   phase-grounding    phase-writing
                                   phase-transforming phase-writing}
                 action-settle    {phase-writing phase-settling}
                 action-quiesce   {phase-settling phase-quiescent}}})


(def create-task-latch
  "Creates a MachineLatch with schema for Task lifecycle.

   Begins in pending phase."
  (ml/machine-latch-factory state-machine))
