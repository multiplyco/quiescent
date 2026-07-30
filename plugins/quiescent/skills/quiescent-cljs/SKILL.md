---
name: quiescent-cljs
description: >-
  Use when writing ClojureScript or cross-platform .cljc code with Quiescent
  (co.multiply.quiescent). Covers what the JS runtime changes: no blocking
  deref, as-jsp, no cpu-task or -cpu variants, gate instead of semaphore,
  abort-signal / aborted? / comply-abort for Fetch, predicate-based catch,
  and cancellation as ex-info rather than CancellationException.
---

# Quiescent on ClojureScript

Companion to the `quiescent` skill, which covers the model itself — tasks, grounding, structured concurrency,
cancellation, the handler tables. **Read that first**; everything in it holds on both platforms unless contradicted
here. This file is only the delta introduced by the JavaScript runtime.

Quiescent aims for identical semantics on both platforms. The differences below are forced by the runtime, not chosen.

## No blocking deref

JavaScript is single-threaded, so tasks cannot be dereferenced with `@`. Chain handlers, convert to a JS Promise with
`q/as-jsp`, or use the `with-task` test helper.

```clojure
;; Won't work in CLJS:
;; @(q/task :result)

(-> (q/task :result)
  (q/then process-result)
  (q/catch handle-error))

(-> (q/as-jsp my-task)
  (.then process-result)
  (.catch handle-error))
```

This is the single largest practical difference. Patterns that read naturally in CLJ — `@(q/compel ...)` inside a
side-effect handler to order downstream after an effect, the `q/ok` + `@` system-boundary idiom — have **no CLJS
equivalent**. Restructure instead: `qlet` + `qdo` expresses the same ordering and works on both platforms.

## No virtual/platform thread distinction

Not available in CLJS:

- `cpu-task` — use `task` instead (runs on the event loop)
- All `-cpu` handler variants (`then-cpu`, `catch-cpu`, `handle-cpu`, `ok-cpu`, `err-cpu`, `done-cpu`, `finally-cpu`) —
  use the plain versions
- `semaphore`, `with-semaphore`, `acquire`, `release` — use `gate`/`gate-task` instead
- `interrupted?`, `comply-interrupt` — use `aborted?`/`comply-abort` with `abort-signal`
- `deref-cpu`, `throw-on-platform-park!` — not applicable

Because there is no thread pool to exhaust, `gate` is the only concurrency limiter you need, and it behaves as
documented in the main skill (including permit-scoped reentrancy).

## AbortSignal integration (CLJS only)

`q/abort-signal` bridges cancellation into the Fetch API and other AbortController-aware APIs:

```clojure
(qlet [response (js/fetch url #js {:signal (q/abort-signal)})]
  (process response))
```

The signal aborts when the task **settles** — completes, fails or is cancelled — so a cancelled task aborts its
in-flight fetch. Each call creates a fresh `AbortController` tied to the current task, and **throws if called outside a
task scope**. Related: `(q/aborted? signal)` checks whether it has fired; `(q/comply-abort signal)` throws if it has.

## Exception handling differences

Cancellation is a `java.util.concurrent.CancellationException` in CLJ, and an `ex-info` carrying `{:cancelled true}` in
ex-data in CLJS. `q/cancelled?` works uniformly on both — prefer it over inspecting the exception.

`catch` clause matching differs — classes in CLJ, predicates in CLJS:

```clojure
;; CLJ
(q/catch task
  IllegalArgumentException (fn [e] :bad-arg)
  IOException (fn [e] :io-error))

;; CLJS
(q/catch task
  #(= :bad-arg (:type (ex-data %))) (fn [e] :bad-arg)
  #(= :io-error (:type (ex-data %))) (fn [e] :io-error))
```

The single-arity form `(q/catch task handler-fn)` is identical on both platforms. In `.cljc` files, the multi-clause
form needs a reader conditional; the single-arity form does not.

## Integration

```clojure
(q/as-task (js/fetch url))    ; js/Promise -> task
(q/as-jsp my-task)            ; task -> js/Promise
```

`core.async` works on both platforms via `co.multiply.quiescent.adapter.core-async`, and still requires explicit
conversion — channels are not auto-converted during grounding.

## Scoped values

On JDK 25+ Quiescent uses `ScopedValue`. CLJS (and earlier JDKs) use an alternative mechanism; the `scoping`/`ask` API
is unchanged.
