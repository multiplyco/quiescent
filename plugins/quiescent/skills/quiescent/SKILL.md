---
name: quiescent
description: >-
  Use when working with async task coordination using Quiescent
  (co.multiply.quiescent). Covers q/task, q/then, qlet, qfor,
  qdo, qjoin, q/ok, q/catch, q/handle, q/finally, q/race, q/any-of,
  q/gate, q/retry, q/compel, q/promise, structured concurrency,
  grounding and cancellation.
---

# Quiescent

Clojure/ClojureScript library for composable async tasks with automatic parallelization, structured concurrency, and
parent-child/chain cancellation.

**Repository**: https://github.com/multiplyco/quiescent —
`co.multiply/quiescent {:mvn/version "0.8.1"}`, JDK 21+ (25 recommended), Clojure 1.12+.

This file covers the semantics you cannot infer — the ones where a reasonable guess from other async libraries is wrong.
It is not an API reference. **Every public var carries a thorough docstring**, so for signatures, arities and options,
prefer `(clojure.repl/doc q/timeout)` over guessing. Sections marked **See also** name a var and what it is for; read
its docstring when you need it.

When writing `.cljs` or `.cljc`, also load the **`quiescent-cljs`** skill.

## Core Design Constraints

- **A task cannot exceed the lifetime of its parent** (escape hatch: `compel`)
- **A task cannot cross the "border" of another task** — where a task would return another task, a plain value is
  returned instead (auto-flattening)
- **If one task fails within a scope, all siblings are torn down** while stateful cleanup still completes

## Core Invariant: Task Boundaries

Every task boundary guarantees dereferencing returns a concrete value with no nested tasks:

- You will never have `Task[Task[A]]` (nested tasks)
- You will never have data structures with unresolved tasks as leaves
- Tasks in data structures execute in parallel automatically
- `q/then` flattens, enabling clean composition without `flatMap` noise

## Creating Tasks

```clojure
(require '[co.multiply.quiescent :as q :refer [q qdo qjoin qfor qlet qmerge if-qlet when-qlet]])

;; Current thread — for static values/minimal computation
(q value)

;; Virtual thread — for IO/blocking operations (default for most work)
(q/task expr)

;; Platform thread — for compute-intensive work only
(q/cpu-task expr)
```

`task` spawns on a virtual thread, `cpu-task` on a platform thread, `q` runs synchronously. Otherwise semantically
equivalent.

```clojure
;; Already-settled failure — short-circuit without spawning
(q/failed-task (ex-info "Invalid input" {:code 400}))
```

**Style — `:refer` the `q`-prefixed names.** The `q` prefix exists to disambiguate these from `clojure.core` (`for`,
`let`, `do`, `merge`), so alias-qualifying them (`q/qfor`, `q/qlet`) stacks a hat on a hat. Refer them bare — including
`q` itself, plus `if-qlet`/`when-qlet` — and use the `q/` alias for everything else (`q/task`, `q/then`,
`q/catch`, ...).

### Promises

Resolve externally — useful for bridging callback APIs:

```clojure
(def p (q/promise))
(deliver p "result")       ; or (q/fail p (ex-info "oops" {}))
@p ;; => "result"
```

Promises implement the full task API (then, catch, cancel, race, etc.) but cannot be compelled.

## Structured Concurrency

Tasks form a tree. Inner tasks become children of the outer:

```clojure
(def parent
  (q/task
    (let [child (q/task (Thread/sleep 100) :done)]
      @child)))
```

**A child task cannot outlive its parent.** When a parent settles, all unsettled children are cancelled automatically.
This applies recursively to all descendants.

### The `compel` Escape Hatch

Protect tasks that genuinely need to outlive their parent (cleanup, flushing):

```clojure
(q/task
  (q/compel (flush-to-disk))
  :done)
```

The compelled task won't be cancelled when the parent settles. Cascade cancellation stops at the `compel` moat. Direct
cancellation of a compelled task still works if you hold a reference to it.

**Compel the creation, not a task you already hold.** Only an inline-constructed task is itself compelled; given a
pre-existing task, `compel` protects the wrapper alone, leaving the inner task exposed to the cascade that was already
coming for it. The distinction is invisible at the call site and fails silently:

```clojure
;; GOOD — the task is constructed inside compel, so it is compelled
(q/compel (flush-to-disk))

;; BAD — t is already a child of the enclosing scope; only the wrapper is compelled
(let [t (q/task (flush-to-disk))]
  (q/compel t))
```

Promises can never be compelled — `compel` throws on one, since blocking teardown on something externally controlled
would risk a leak.

## Grounding

When a task returns a data structure containing nested tasks, those tasks are resolved in parallel and inlined — called
"grounding":

```clojure
@(q/task
   {:user   (fetch-user 123)      ; Returns Task
    :orders (fetch-orders 123)})   ; Returns Task
;; => {:user {...}, :orders [...]}
```

Grounding is transitive, non-blocking (parent thread returns to pool), and if any task fails, all siblings are cancelled
immediately.

Grounding is a safe operation when performed at the border of`cpu-task`s. The platform thread won't be pinned; it
returns to the pool, and the grounded tasks complete independently.

## Chaining & Transformation

### `then` — Transform success values

```clojure
;; Single task
(-> (q/task (fetch-user 123))
  (q/then :name))

;; Multiple tasks — waits for all in parallel (variadic)
(q/then task-a task-b task-c
  (fn [a b c] (+ a b c)))
```

The function receives dereferenced values from all tasks as separate arguments:

```clojure
(q/then
  (fetch-user user-id)
  (fetch-posts user-id)
  (fetch-comments user-id)
  (fn [user posts comments]
    {:user user :posts posts :comments comments}))
```

### `catch` — Recover from errors

```clojure
(-> (q/task (fetch-user 123))
  (q/then :name)
  (q/catch (fn [e] "Unknown")))

;; Multiple exception types (exclusive-or, like try/catch)
(-> (q/task (risky-operation))
  (q/catch
    IllegalArgumentException (fn [e] :bad-arg)
    IOException (fn [e] :io-error)
    Throwable (fn [e] :other)))
```

### `handle` — Unified success/error handling

```clojure
(-> (q/task (fetch-data))
  (q/handle
    (fn [value error]
      (if error
        {:status :error :message (ex-message error)}
        {:status :ok :data value}))))
```

Check `error` for nil (reliable). Checking `value` for nil is unreliable since nil can be a valid result.

### `qmerge` — Merge maps with task values

```clojure
(qmerge
  {:user (fetch-user id)}
  {:orders (fetch-orders id)})
;; => {:user {...} :orders [...]}
```

## Side Effects

| Handler   | Runs on success | Runs on error | Runs on cancellation | Transforms value |
|-----------|-----------------|---------------|----------------------|------------------|
| `ok`      | yes             |               |                      | no               |
| `err`     |                 | yes           |                      | no               |
| `done`    | yes             | yes           |                      | no               |
| `finally` | yes             | yes           | yes                  | no               |

Side-effect handlers don't alter the outcome — the original value/error passes through. **However, exceptions thrown
from side-effect handlers do propagate and fail the task chain.** (`monitor` is the one exception — see below.)

```clojure
(-> (q/task (fetch-user 123))
  (q/ok (fn [user] (log "Fetched" {:id (:id user)})))
  (q/err (fn [e] (log "Failed" {:error e})))
  (q/finally (fn [_v _e c] (release-resource))))
```

`finally` receives three args: value, error, cancelled flag. It's the only handler that runs on cancellation.

### `monitor` — Trigger side effect on slow tasks

```clojure
(-> (q/task (Thread/sleep 5000) :ok)
  (q/monitor 500 #(println "This is taking a long time.")))
```

Imposes no deadline: it returns the task you gave it and cannot affect the outcome. The side effect fires only if the
task hasn't settled by then, and is skipped once it does.

Unlike the other side-effect handlers, **an exception thrown by the side effect is contained** — it does not propagate
and is not reported anywhere. A bare `log/warn` is the normal usage and needs no protection. If the side effect does
something with consequences beyond reporting, handle its failure inside the function; nothing outside will see it.

### `time` — Measure task duration

```clojure
(-> (fetch-user id)
  (q/time (fn [value error cancelled ms]
            (log/info "fetch-user took" ms "ms"))))
```

### `qdo` — Sequential side effects (async `do`)

Macro. Executes clauses sequentially: each clause is awaited before the next one is evaluated, like an imperative `do`.
Returns a Task with the last clause's value. If a clause fails, the remaining clauses are never evaluated.

```clojure
(qdo
  (mark-pending! id)    ; awaited first
  (upload! id payload)  ; starts only after the previous clause settles
  (mark-active! id))    ; result returned
```

Prefer this over chains of `(q/then ... (fn [_] ...))` that ignore the previous result.

### `qjoin` — Parallel fan-in, return last

Function. Awaits all tasks in parallel and returns only the last one's value. Tasks are hot (already running when passed
in), so `qjoin` controls only the fan-in. If one task fails, all are cancelled.

```clojure
(qjoin
  (log-event)       ; awaited but result discarded
  (perform-work))   ; result returned
```

## Handler Summary

| Handler   | Primary purpose | Runs on success | Runs on error | Runs on cancellation |
|-----------|-----------------|-----------------|---------------|----------------------|
| `then`    | Transformation  | yes             |               |                      |
| `catch`   | Transformation  |                 | yes           |                      |
| `handle`  | Transformation  | yes             | yes           |                      |
| `ok`      | Side-effect     | yes             |               |                      |
| `err`     | Side-effect     |                 | yes           |                      |
| `done`    | Side-effect     | yes             | yes           |                      |
| `finally` | Teardown        | yes             | yes           | yes                  |

**Cancellation is a control signal, not an error.** Only `finally` runs on cancellation.

All handlers have `-cpu` variants (`then-cpu`, `ok-cpu`, etc.) that run on platform threads instead of virtual threads.

## Automatic Parallelization with `qlet`

Analyzes symbol dependencies and parallelizes independent bindings automatically:

```clojure
@(qlet [user (fetch-user id)            ; starts immediately
        posts (fetch-posts id)            ; parallel with user (independent)
        promos (fetch-promotions)           ; parallel with both (independent)
        profile (process-user user)         ; waits for user
        result (build-dashboard profile posts promos)]  ; waits for all deps
   result)
```

**How it works:**

- Builds a dependency graph (DAG) from symbol references
- Independent operations run in parallel
- Supports all Clojure destructuring (maps, vectors, rest, nested)
- Returns a Task — use `@` to deref or chain with `q/then`

**Important:** Forms with no dependencies are not automatically wrapped in tasks. Supply your own `task`/`cpu-task` for
inline task construction.

**See also** `if-qlet` and `when-qlet` — await, bind if truthy, else branch / execute body. They are exactly the
`clojure.core` forms with `qlet` binding semantics.

## `qfor` — Parallel mapping

```clojure
;; For task-returning functions: auto-flattens
(qfor [id ids]
  (fetch-user id))

;; For blocking operations: wrap in q/task
(qfor [batch batches]
  (q/task
    (Thread/sleep wait)
    (process-batch batch)))

;; The collection may itself be a task
(qfor [user (fetch-users)]
  (enrich user))
```

Mapping runs on a **single virtual thread**, elements in order. The body may park (deref, await), but a blocking body
serializes the loop — wrap it in `q/task` to run elements in parallel.

Like a `qlet` binding, the collection expression is **resolved before mapping**: it may be a task, and any task
elements are grounded first, so the body always sees plain values. To map over tasks *as* tasks, use ordinary Clojure:
`(q (mapv f tasks))`.

Unlike `clojure.core/for`, `qfor` takes a **single** binding pair and supports **no** `:let`, `:when` or `:while`
modifiers. Filter the collection before the loop, or bind inside the body.

## Cancellation

```clojure
(def my-task (q/task (slow-operation)))
(q/cancel my-task)  ; Returns task resolving to boolean (true=cancelled, false=already settled)
```

A cancelled task contains a `CancellationException` and throws if dereferenced.

**See also** for reading a task without `deref`'s throwing behaviour:

- `(q/await task)` / `(q/await task 1000)` — a task settling to true at quiescence, or false if the bound expired
  (deref to block; CLJ only). Never throws on failure or cancellation, and never touches the observed task: an expired
  bound does not cancel it — `await` observes where `timeout` owns.
- `(q/get-now task :not-ready)` — the value if settled, else the default. Does throw if it settled exceptionally.

### Detecting cancellation

```clojure
(q/cancelled? some-task)  ; => true or false

;; Inside finally:
(q/finally task
  (fn [_v _e c]
    (when c (log/info "Task was cancelled"))))
```

**See also** the predicates `q/task?`, `q/promise?`, `q/exceptional?` (settled with an exception, without dereferencing)
and `q/taskable?` — true for anything Quiescent handles specially: Task, Promise, CompletableFuture and Future (CLJ),
`js/Promise` (CLJS), and core.async channels once the adapter ns is required.

## Controlling Parallelism

### Gates

Limit concurrent tasks:

```clojure
(let [g (q/gate 20)]
  @(qfor [n (range 1000)]
     (q/gate-task g
       (process n))))
```

Gates participate in structured concurrency: cancelling a gate cancels every task enqueued through it, and a gated task
is also structured under whatever created it, so cascade cancellation from its creator reaches it. Wrap in `compel` to
detach fire-and-forget gated work from its creator.

Gates are **reentrant while the permit is held**: a `gate-task` created inside a gated body runs immediately on the
enclosing task's permit, so awaiting a nested `gate-task` cannot deadlock the gate. Once the enclosing task settles and
releases its permit, descendants acquire normally like any other caller — reentrancy does not outlive the permit.

Handlers chained onto `gate-task` run *after* the permit is returned.

**See also** `q/semaphore` / `q/with-semaphore` (CLJ only) — a plain counting semaphore for guarding a blocking section,
where `gate` limits task admission. Prefer `gate` unless you specifically need the lock shape.

## Retry

`(q/retry f opts)` calls `(f retrying?)` with exponential backoff. Options: `:retries` (default 3), `:backoff-ms`
(default 2000), `:backoff-factor` (default 2), `:retry-callback` `(fn [e retries backoff])`, and `:validate`
`(fn [value error])` — throw from `:validate` to treat a *successful* result as a failure worth retrying, which is how
you retry on e.g. a 503 response body.

## Deadlines

**See also** `(q/sleep ms)` — a task that settles after a delay, optionally with a value: `(q/sleep 500 :expired)`.
Durations are ms or `java.time.Duration`, must be non-negative (0 settles immediately), and anything else throws.

Both `sleep` and `timeout` interpret their `default` polymorphically: **a fn is called** at the deadline, **a Throwable
is thrown**, anything else is used as the value.

### `timeout` — Bound a task by a deadline

```clojure
(q/timeout t 5000)                  ; throws TimeoutException at the deadline
(q/timeout t (Exception. "Oh no!")) ; custom exception thrown at the deadline
(q/timeout t 5000 :gave-up)         ; settles with a value instead
(q/timeout t 5000 #(fallback))      ; calls a fn — may itself return a task
(q/timeout t (Duration/ofMinutes 5))
```

Behaviours you can assume:

- **`default` covers the timer firing and nothing else.** A task that *fails* throws its own exception, promptly, even
  when a `default` was supplied. To absorb failures too, say so explicitly:
  `(-> t (q/timeout ms fallback) (q/catch (fn [_] fallback)))`.
- **The awaited task prevails the moment it settles**, however it settles. One already settled when `timeout` is called
  wins outright, whatever the duration.
- **Cancellation passes through as cancellation**, carrying the original exception — not as a timeout. Cancelling the
  task returned by `timeout` cancels the awaited task; the timer is torn down as soon as the awaited task settles, so it
  never outlives the work.
- Expiry cascade-cancels the awaited chain, so `q/finally` cleanup still runs.

## Racing

```clojure
;; Race individual tasks — first *successful* result wins, losers are cancelled
(q/race task-a task-b task-c)

;; Race data structures (first group to fully ground wins)
(q/race #{t1 t2} #{t2 t3} #{t1 t3})

;; race-stateful: cleanup function for realized-but-losing tasks
(q/race-stateful Socket/.close task-a task-b)

;; any-of: like race, but observes instead of owns — losers keep running,
;; and cancelling the result does not propagate to the entrants. Use for
;; tasks that are shared or owned elsewhere.
(q/any-of cache-lookup fresh-fetch)
```

**Naming the winner.** A race settles with the winner's *value* — the winning task's identity is not in the result. Tag
entrants with data structures to recover it: `(q/race [:eu eu-fetch] [:us us-fetch])` → `[:us result]`. Each vector
grounds exactly when its task settles, so race ordering is unchanged. With `race-stateful`, `release` then receives the
tagged pair, not the raw resource: `(q/race-stateful #(-> % last Socket/.close) [:a conn-a] [:b conn-b])`.

**"All of" needs no function**: a data structure is one — `[t1 t2 t3]` grounds when every task has completed.

## Scoped Values

Uses [Scoped](https://github.com/multiplyco/scoped) for efficient scope propagation (ScopedValue on JDK 25+):

```clojure
(require '[co.multiply.scoped :refer [ask scoping]])

(def ^:dynamic *name*)

(scoping [*name* "Alice"]
  (q/task
    (println "Hello," (ask *name*))))
```

## Integration

`CompletableFuture` and `Future` (CLJ) and `js/Promise` (CLJS) are **auto-converted** — returned from a task body or
passed to a chaining function, they just work:

```clojure
(qlet [result (s3/async-get ...)]   ; CompletableFuture, no conversion needed
  (process result))
```

Convert explicitly with `q/as-task` (in) and `q/as-cf` / `q/as-jsp` (out).

`core.async` is the exception — channels are **not** auto-converted during grounding, so conversion is always explicit.
Require `co.multiply.quiescent.adapter.core-async`, then `q/as-task` for channel → task and the adapter's
`as-chan` for task → channel.

## Platform Thread Safety

Dereferencing on a platform thread throws by default (prevents blocking carrier threads):

```clojure
@my-task              ; throws "Refusing to park platform thread"
(q/deref-cpu my-task) ; explicitly allow blocking deref
(q/throw-on-platform-park! false) ; disable check globally
```

## Task Reusability

Tasks are values, not one-shot channels: once settled they hold their result permanently. A task can be chained from
repeatedly and dereferenced any number of times, and each chain is independent — `(q/then a f1)` and `(q/then a f2)`
both see `a`'s value without re-running it.

---

# Patterns

## Strongly prefer task composition over dereferencing

**Avoid using `@` (deref) on a Task inside any Quiescent callback** (`q/then`, `q/ok`, `q/handle`, `q/catch`,
`q/finally`, `qlet`, `qfor`, etc.). Derefing blocks the thread and defeats async coordination.

There are occasions where one might need to break this rule, but it should have a strong motivation. For example: test
assertions where you need to unwrap a value for use outside the task graph.

Quiescent task grounding and task composition provides a strong foundation to express the vast majority of patterns
without dereferencing.

```clojure
;; Generally BAD: Blocking deref inside q/then
(-> (fetch-user id)
  (q/then (fn [user]
            @(save-audit-log user))))  ; Blocks thread

;; Generally GOOD: Return the task — automatically grounds
(-> (fetch-user id)
  (q/then (fn [user]
            (save-audit-log user))))   ; Returns Task
```

Side-effect handlers are the exception worth knowing separately — they neither ground nor await what the handler
returns, so both the rule and its escape hatches differ. See
[Launching a task inside a side-effect handler](#launching-a-task-inside-a-side-effect-handler).

## Launching a task inside a side-effect handler

`ok`, `err`, `done` and `finally` **ignore what the handler returns** — the original outcome passes through the moment
the handler *returns*, not when work it started settles. This is deliberate: it lets a handler kick off something
open-ended and move on. It also means a bare task created inside one is an ordinary child of a link that settles
immediately, so it is cancelled before it can run — and nothing throws or logs, the effect simply doesn't happen.

Two **independent** questions decide the fix: must the effect survive cancellation of the chain (`compel`), and must
downstream observe it (await)?

| Inside a side-effect handler | Survives cancellation        | Downstream observes |
|------------------------------|------------------------------|---------------------|
| `(persist! ...)`             | no — never runs at all       | no                  |
| `(q/compel (persist! ...))`  | yes                          | no                  |
| `@(persist! ...)`            | **no** — torn down mid-write | yes                 |
| `@(q/compel (persist! ...))` | yes                          | yes                 |

Row three is the subtle one: a bare `@` orders downstream correctly, but the awaited task is still an ordinary child, so
cancelling the chain tears it down **mid-write**. Writing it asserts a half-completed `persist!` is acceptable — for a
persist it rarely is. Prefer `@(q/compel ...)` unless the effect is genuinely abandonable. (`@` is CLJ only; it parks a
virtual thread, which is cheap. On `finally`'s cancellation path there is no downstream to order, so only the
`compel` axis is meaningful there.)

If you only need ordering and the effect *is* abandonable, restructure instead of blocking — a side-effect handler
cannot express ordering at all, whereas `qlet` + `qdo` says it directly and works in CLJS:

```clojure
;; qdo awaits each clause, returns the last — so `value` still passes through.
(qlet [value (q/task 42)]
  (qdo
    (persist! store data)
    value))
```

## Loan pattern for resource lifecycle

The async counterpart to `with-open`: open a resource, lend it to `f`, and release it however `f` settles. `finally`
is the handler for this — it is the only one that also runs on **cancellation**, so a resource is released even when the
task tree is torn down from above.

```clojure
(defn with-stream
  "Open a stream, lend it to f, and close it however f settles."
  [path f]
  (let [s (io/input-stream path)]
    (-> (f s)
      (q/finally (fn [& _] (InputStream/.close s))))))

;; Usage
(with-stream "data.bin"
  (fn [s]
    (q/task (parse-records s))))
```

Ignore the handler's arguments — value, error and cancelled flag are irrelevant when the answer is "close it either
way". The resource passes through unchanged: `finally` doesn't transform, so callers see `f`'s own result.

- The resource is bound outside the chain so the handler can close it regardless of how `f` settles
- Release survives success, failure **and** cancellation — the case `ok`/`done` would miss
- Callers never see the resource's lifecycle, only its result
- A synchronous `close` sidesteps the return-value trap above entirely; if release is itself async, wrap its
  **creation** in `compel` — `(q/compel (close-async! s))` — since `finally` ignores what the handler returns and the
  chain is already settling

# ClojureScript Differences

Quiescent runs on both platforms with the same semantics, but the JS runtime forces a handful of differences — no
blocking deref (so no `@`), no `cpu-task` or `-cpu` variants, `gate` instead of `semaphore`, predicate-based `catch`,
cancellation as `ex-info` rather than `CancellationException`, and `abort-signal` for Fetch.

**When writing ClojureScript, load the `quiescent-cljs` skill** — it covers the full delta. The most common trap is
reaching for a CLJ-only pattern from this file: anything using `@` needs restructuring, typically to `qlet` + `qdo`.
