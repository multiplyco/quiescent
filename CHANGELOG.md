# Changelog

## 0.6.0 - 2026-07-27

- **BREAKING**: an exception thrown by a `monitor` side effect is now contained rather than propagated. Previously it
  failed the returned task *and* cancelled the task being monitored — so a diagnostic could destroy the work it was
  only supposed to observe. This was consistent with the side-effect handlers (`ok`, `err`, `done`, `finally`), whose
  exceptions do fail the chain, but the consequence is not comparable: those run once a task has settled, where
  propagating can only affect what happens next, whereas a monitor fires while the task is still running. Worse, the
  side effect runs only when things are slow, so a latent bug in one lay dormant until the system was under load and
  then took out the very work it was added to observe. Nothing reports the exception now either — Quiescent carries
  no logging of its own, and since the side effect is nearly always a log line, a call site could not have handled it
  regardless: there is nothing to write in the `catch` block when logging is what failed. Side effects with
  consequences beyond reporting should handle their own failures.
- **Changed**: `monitor` returns the task it was given, rather than a wrapper around it. It no longer builds on
  `timeout`, which required a `compel` moat purely to disarm the cancellation that `timeout` exists to perform; it is
  now a timer torn down the moment the monitored task settles. Roughly halves the per-call overhead, and makes the
  documented promise that `monitor` cannot affect the outcome structural rather than a matter of care.

## 0.5.1 - 2026-07-27

- **Performance**: `timeout` no longer re-grounds the value of the task it bounds. A settled task's result is already
  grounded — the task boundary guarantees no inner task survives it — but `timeout` was re-applying that value, which
  walked the entire structure a second time looking for tasks that cannot be there. The cost scaled with the size of
  the value: bounding a task that succeeded with a ~10k-node structure cost ~1.4ms per call in overhead alone. The
  settled state is now adopted as-is, so the overhead is flat at a few microseconds regardless of the value's size.
  Values that aren't traversed structurally, such as strings, were never affected by the scaling and gain only the
  smaller saving from a shorter chain.
- **Guaranteed**: a task that has already settled when `timeout` is called now wins outright, whatever the duration —
  including a zero deadline. This held in practice before and is now structural: the task is subscribed before the
  timer is constructed, so an already-settled task claims the result synchronously, with nothing yet in existence to
  contest it.
- **Changed**: cancelling the awaited task now surfaces that task's own `CancellationException`, rather than a fresh
  one describing a race that callers were never party to. Cancellation was already reported as cancellation (see
  0.5.0); only the exception's identity and message change.
- Internally, `timeout` no longer builds on `race`. A deadline is not a race — the timer is a watchdog guaranteed to
  succeed eventually, not a competitor doing the same work — and expressing it as one required wrapping the awaited
  task's exceptions as values to stop the timer winning by walkover, then unwrapping them in a transform. Both that
  wrapper and the sentinel marking the timer's own result are gone; the task and the timer now settle a shared result
  directly, the first to arrive claiming it.

## 0.5.0 - 2026-07-26

- **BREAKING**: `timeout` no longer resolves to its `default` when the awaited task *fails*. `default` now covers
  the timer firing and nothing else; a failing task throws its own exception, promptly. Previously `timeout` was
  built on `race`, which is decided by the first non-exceptional result — and since the timer is not a competitor
  but a watchdog guaranteed to succeed eventually, it won by walkover whenever the task failed. The consequences
  were that every failure under a `timeout` cost the full timeout to report, the original exception was discarded
  outright (no cause, no data, not chained) in favour of a `TimeoutException`, and — least obviously — a `timeout`
  anywhere between a failure and its handler silently disabled that handler, since the exception never reached it.
  Call sites that relied on a `default` absorbing errors as well as slowness need an explicit `catch`:
  `(-> t (timeout ms fallback) (catch (fn [_] fallback)))`, which also reports failures without waiting out the
  deadline.
- **Changed**: cancelling the task awaited by a `timeout` now settles the timeout as *cancelled*, rather than
  reporting a timeout at the deadline. Cancellation cascades from parent to child, not between siblings, so the
  timer previously kept running after the task it raced was cancelled and went on to win. The timer is now torn
  down as soon as the awaited task settles, for any reason, and so never outlives the work it was watching.
  Cancelling the task returned by `timeout` continues to cancel the awaited task, as before.

## 0.4.0 - 2026-07-04

- Fix a stack overflow that could permanently wedge a gate. Draining a long run of tasks cancelled while queued
  recursed one stack frame set per settled entry; at ~10k consecutive cancelled entries the overflow was swallowed
  by the subscription runner, the permit was lost, and every subsequent `gate-task` queued forever. The queue now
  skips settled entries iteratively.
- **Changed**: gate reentrancy now expires with its permit. A `gate-task` created within a gate's scope runs
  immediately only while the enclosing gated task still holds its permit — awaited nested enqueues still can't
  deadlock the gate. Once the holder settles (provably releasing the permit), descendants — e.g. compelled
  fire-and-forget tasks — acquire a permit like any other caller. Previously, anything created under the gate's
  scope bypassed the concurrency limit indefinitely.
- **Changed**: gated tasks are now structured under the task that created them, like any other task: when the
  creator settles, cascade cancellation reaches fire-and-forget gated work. The gate still owns every task enqueued
  through it — cancelling the gate cancels them all. Wrap with `compel` to detach gated work from its creator
  (the previous behavior).

## 0.3.0 - 2026-06-11

- **BREAKING**: `qdo` is now a macro with sequential semantics, like an async `do`: each clause is evaluated only
  after the previous one has settled, and a failure skips the remaining clauses. Previously, `qdo` was a function
  whose (hot, already-running) task arguments were awaited in parallel, with all of them cancelled if one failed.
  That behavior lives on unchanged as `qjoin` (new). Existing `qdo` call sites are source-compatible — clauses are
  still all awaited and the last value returned — but clauses no longer run concurrently; switch to `qjoin` where
  parallel execution was intended.

## 0.2.6 - 2026-05-26

- Fix `ClassNotFoundException: clojure.lang.AFunction` (and similar lazy class-generation / reflection failures) on
  executor worker threads. Worker threads are constructed on whichever thread submits the first task and previously
  inherited that thread's context classloader; when the submitter's loader couldn't resolve Clojure's own classes
  (e.g. a Rama module/daemon thread), lazy class generation on the worker — Specter dynamic-path eval, runtime
  reflection, agent fns — would fail. All three executors (`q-io`, `q-cpu`, `q-se`) now pin the classloader captured
  at namespace load as each worker's context classloader.

## 0.2.5 - 2026-02-23

- Fix performance warning in `ground`: `case` expression now coerces to primitive int, avoiding boxed comparison.
- Add `bb warn:reflection` task for checking reflection and performance warnings. Runs as part of `bb test:clj`.

## 0.2.4 - 2026-02-09

- **ClojureScript support.** The core task API now works on both Clojure and ClojureScript. See README for
  platform-specific differences.
- `gate` / `gate-task` (new); cross-platform concurrency limiter. Creates a gate with N permits; `gate-task` runs
  work through the gate, queueing if no permits are available. Gates participate in structured concurrency (cancelling
  a gate cancels all gated tasks) and are reentrant (nested `gate-task` calls on the same gate don't consume additional
  permits).
- `cancel` now accepts any `ICancellable`, not just tasks. This allows cancelling gates directly.
- Promises can now be cancelled. Cancelling a promise sets the `cancelled?` flag and propagates cancellation (not
  failure) to chained tasks, consistent with task cancellation semantics.
- `abort-signal` / `aborted?` / `comply-abort` (new, CLJS only); create an `AbortSignal` tied to the current task's
  lifecycle. The signal is aborted when the task settles (completes, fails, or is cancelled), enabling automatic cleanup
  of `fetch` requests and other AbortSignal-aware APIs.
- `as-jsp` (new, CLJS only); convert a task to a JavaScript Promise. The CLJS counterpart to `as-cf`.
- Task coordination performance improvements. The subscription system now uses a lock-free intrusive deque instead of
  a `ConcurrentHashMap`, improving scalability when many sibling tasks are started concurrently within a single parent.
  Except for `race`, the entire implementation is now lock-free.

## 0.1.14 - 2026-01-15

- Docstring formatting improvements for cljdoc rendering (Args sections now use markdown lists).
- Added function examples to `sleep` and `timeout` docstrings.
- Renamed `err1/err2/...` parameters to `e1/e2/...` in `catch` and `catch-cpu` for consistency.

## 0.1.12 - 2026-01-14

- `race-stateful`; Fixed bug where racing the same task multiple times could incorrectly release the winning result, or
  could attempt to release the same losing result multiple times. Now, winning results will not be released, and losing
  results will be released exactly once. Introduces a constraint: `release` will not run on `nil` due to underlying
  constraints. This is OK, `nil` is not stateful.

## 0.1.11 - 2026-01-13

- `time` (new); measure the time it takes to execute a task. Takes a function that receives the args of `finally` plus a
  Duration that estimates the time it took to run the task.
- `monitor` no longer incorrectly breaks cascading cancellation chain.

## 0.1.10 - 2026-01-12

- CPU executor now uses a work-stealing `ForkJoinPool` instead of a fixed thread pool, since it's reasonable to expect
  CPU bound work of variable duration.
- CPU executor pool size reduced from `2 * number-of-cores` to `number-of-cores`. With platform parking throwing by
  default, the previous precaution should be unnecessary.
- Cancelled `CompletableFuture` now propagates as task cancellation rather than exception. Previously, cancellations
  would propagate as if they were exceptions, causing the Quiescent task to go into exception handling (e.g. `catch`)
  rather than exclusively teardown handling (`finally`).

## 0.1.9 - 2026-01-08

- Bump `pathling` to 0.1.8

## 0.1.8 - 2026-01-03

- Bump `scoped` to 0.1.14
- Bump `machine-latch` to 0.1.12

## 0.1.7 - 2026-01-05

- `race` now cancels the "winner" task if all participants in the race are cancelled.
- `await` does not explicitly take a phase to await. It's now equivalent to `deref`, but returns a boolean rather than
  returning a value or throwing an exception.

## 0.1.6 - 2026-01-05

### Replace weak references with mutual cleanup

Previously, tasks occasionally held a weak reference to one another to allow for GC to occur even though eg. a parent
held a reference (for potential cancellation) to a child.

This is now replaced with a mutual cleanup instead. For task A and B, if A wants to be able to cancel B, it will have a
strong reference to B. But if B passes the stage where it can be cancelled, it will remove this subscription from A.

This is also more performant than hanging on to the weak references, and estimated performance numbers have been lowered
in accordance.

## 0.1.5 - 2026-01-03

- Bump `scoped` to 0.1.13

## 0.1.4 - 2026-01-02

Initial release.