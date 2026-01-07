# Changelog

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