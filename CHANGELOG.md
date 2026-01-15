# Changelog

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