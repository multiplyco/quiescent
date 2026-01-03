# Quiescent

[![Clojars Project](https://img.shields.io/clojars/v/co.multiply/quiescent.svg)](https://clojars.org/co.multiply/quiescent)
[![cljdoc](https://cljdoc.org/badge/co.multiply/quiescent)](https://cljdoc.org/d/co.multiply/quiescent)

A Clojure library for composable async tasks with automatic parallelization, structured concurrency, and parent-child
and chain cancellation.

In particular, Quiescent has been designed with these constraints in mind:

- **A task cannot exceed the lifetime of its parent**. Though there's an escape hatch for things that need to run to
  completion.
- **A task cannot cross the "border" of another task**. Where a task would return another task, a plain value is
  returned instead.
- **If one task fails within a scope, all siblings should be torn down as quickly as possible** while teardown of
  statefully allocated resources should still complete without interruption.

Tasks go through 7 phases during their lifecycle: pending, running, grounding, transforming, writing, settling and
**quiescent**. This library is named after the last phase in the lifecycle, which is when all subtasks have settled and
the task tree has come to rest.

## Requirements

Quiescent builds heavily on virtual threads and other features present only in recent versions of the JDK.

- JDK 21+ (Virtual threads)
- JDK 25 recommended (Better virtual threads; `ScopedValue` support)
- Clojure 1.12+

## Installation

```clojure
;; deps.edn
co.multiply/quiescent {:mvn/version "0.1.5"}
```

## Core concepts

### Tasks

A task represents an async computation. Create one that executes on a virtual thread with `task`:

```clojure
(require '[co.multiply.quiescent :as q])

;; Quiescent strongly discourages parking on platform threads.
;; Let's turn it off for this REPL session, or we won't be able to dereference
;; the tasks that we create.
(q/throw-on-platform-park! false)

(def my-task
  (q/task
    (Thread/sleep 100)
    "done"))

@my-task
;; => "done"
;; (blocks until complete)
```

`task` spawns a task that runs on a virtual thread, while `cpu-task` runs directly on a platform thread, and `q` runs
on the calling thread. Other than that, they are all semantically equivalent.

### Promises

Promises are tasks you resolve externally, which is useful for bridging callback APIs:

```clojure
(def p (q/promise))

; Later, from a callback:
(deliver p "result")
; or
(q/fail p (ex-info "oops" {}))

@p
;; => "result"
```

Promises cannot be cancelled or compelled, but otherwise implement the full task API. You can chain with `then`,
`finally`, and so on, or `race` them against other tasks, or do anything else that tasks generally permit.

### Structured Concurrency

Tasks form a tree. When you create a task inside another task, the inner task becomes a child of the outer:

```clojure
(def parent
  (q/task
    (let [child (q/task (Thread/sleep 100) :done)]
      @child)))
```

**A child task cannot outlive its parent.** When a parent settles (completes, fails, or is cancelled), all children that
haven't yet settled are cancelled automatically.

```clojure
(def parent
  (q/task
    (q/task (Thread/sleep 5000) (println "I'll never print"))
    :done)) ;; Parent returns immediately

@parent
;; => :done
;; The inner task is cancelled—it never prints.
```

The inner task here is an orphan: it was started but not awaited or returned. When the parent settles with `:done`, the
orphan is torn down.

This applies recursively. If a grandparent settles, all descendants—children, grandchildren, and so on—are cancelled.

Structured concurrency prevents resource leaks and runaway tasks. You don't need to manually track and cancel background
work. Settling the parent cleans up the entire subtree.

It also means exceptions propagate predictably. If a child fails and the parent doesn't handle it, the parent fails too,
which cancels all siblings.

#### The `compel` escape hatch

Sometimes a task genuinely needs to outlive its parent (cleanup work, flushing buffers, releasing connections). Use
`compel` to protect it:

```clojure
(q/task
  (q/compel (flush-to-disk))
  :done)
```

The `compel`ed task won't be cancelled when the parent settles. It runs to completion (or failure) independently.
Cascade cancellation from ancestors stops at the moat created by `compel`.

### Grounding

When a task returns a data structure containing nested tasks, those tasks aren't orphans but part of the result.
Their values are resolved in parallel and inlined into the final structure in a process called "grounding."

```clojure
(def result
  (q/task
    {:user   (fetch-user 123)
     :orders (fetch-orders 123)})) ;; <- `fetch-…` returns tasks.

@result
;; => {:user   {...}
;;     :orders [...]}
```

This is transitive. If a returned task itself returns nested tasks, or they chain off neighbouring tasks, they are all
recursively grounded until a plain value is all that remains.

If any task fails during grounding, all siblings are cancelled immediately.

Grounding is not a blocking operation. When grounding begins, the parent thread returns to the pool. The nested tasks
coordinate among themselves to assemble the final value. No control thread waits for them to complete.

This means platform threads are safe to use:

```clojure
(def result
  (q/cpu-task ;; <- Platform thread
    {:user   (q/task (fetch-user 123))
     :orders (q/task (fetch-orders 123))}))
```

The platform thread constructs the map and immediately returns to the pool. The nested virtual-thread tasks complete on
their own and finalize the result. The platform thread never parks.

### Handling outcomes and chaining transformations

Quiescent provides several handlers for reacting to task outcomes:

| Handler   | Primary purpose | Runs on success | Runs on error | Runs on cancellation |
|-----------|-----------------|-----------------|---------------|----------------------|
| `then`    | Transformation  | ✓               |               |                      |
| `catch`   | Transformation  |                 | ✓             |                      |
| `handle`  | Transformation  | ✓               | ✓             |                      |
| `ok`      | Side-effect     | ✓               |               |                      |
| `err`     | Side-effect     |                 | ✓             |                      |
| `done`    | Side-effect     | ✓               | ✓             |                      |
| `finally` | Teardown        | ✓               | ✓             | ✓                    |

- **Transformation**: Transform the outcome. The handler's return value becomes the new result.
- **Side-effect**: Observe the outcome. The original value/error passes through unchanged.
- **Teardown**: Release stateful resources, or other effects that should happen regardless of how the task terminates.

**Cancellation is a control signal, not an error.** When a task is cancelled, only `finally` runs, the rest are torn
down without executing their workload or, if they were in the process of executing their workload, their backing threads
will be interrupted.

### Transformations and error handling

Chain transformations with `then`:

```clojure
(-> (q/task (fetch-user 123))
  (q/then :name))
```

`then` accepts multiple arguments for combining tasks:

```clojure
(q/then task-a task-b task-c
  (fn [a b c]
    (+ a b c)))
```

You're expected to provide a function that accepts as many args as there are tasks. Of course, in this case `+` could be
provided directly.

```clojure
;; Arguably a better version of the above
(q/then task-a task-b task-c +)
```

Use `catch` to recover from errors:

```clojure
(-> (q/task (fetch-user 123))
  (q/then :name)
  (q/catch (fn [e] "Unknown")))
```

`catch` supports multiple exception types with exclusive-or semantics, like `try`/`catch`:

```clojure
(-> (q/task (risky-operation))
  (q/catch
    IllegalArgumentException (fn [e] :bad-arg)
    IOException (fn [e] :io-error)
    Throwable (fn [e] :other)))
```

Only the first matching handler runs. If that handler throws, the exception propagates. It's not caught by later
handlers in the same expression.

For nesting semantics (where one handler's exception can be caught by another), chain separate `catch` calls.

Use `handle` when you need to handle both success and failure:

```clojure
(-> (q/task (fetch-data))
  (q/handle
    (fn [value error]
      (if error
        {:status :error :message (ex-message error)}
        {:status :ok :data value}))))
```

It's idiomatic to check for the presence or absence of `error`, where `nil` reliably indicates the absence of an error.
Checking `nil` on `value` is unreliable, since that can be a valid result.

#### Side effects

`ok`, `err`, `done`, and `finally` produce side effects and can't alter the outcome:

```clojure
(-> (q/task (fetch-user 123))
  (q/ok (fn [user] (log "Fetched user" {:id (:id user)})))
  (q/err (fn [e] (log "Failed to fetch user" {:error e})))
  (q/done (fn [_v _e] (log "`fetch-user` completed.")))
  (q/finally (fn [_v _e _c] (log "`fetch-user` terminated."))))
```

Use `done` to observe success or error (but not cancellation):

```clojure
(-> (q/task (fetch-user 123))
  (q/done (fn [v e]
            (if e
              (metrics/record-failure)
              (metrics/record-success)))))
```

Use `finally` to release resources that must be cleaned up regardless of outcome. It receives a third
argument indicating whether the task was cancelled:

```clojure
(let [resource (acquire-resource)]
  (-> (q/task (use-resource resource))
    (q/finally (fn [_v e c]
                 (release-resource resource)
                 (cond
                   e (println "Error!")
                   c (println "Cancelled!")
                   :else (println "Success!"))))))
```

Use `monitor` to trigger a side effect when a task doesn't finish within the specified timeframe:

```clojure
(-> (q/task (Thread/sleep 5000) :ok)
  (q/monitor 500 #(println "This is taking a long time.")))
```

### Cancellation

Tasks can be cancelled explicitly with `cancel`:

```clojure
(def my-task (q/task (slow-operation)))

(q/cancel my-task)
;; Cancels my-task and all its descendants.
```

`cancel` returns a task that resolves to a boolean: `true` if cancellation succeeded, `false` if the task had already
settled. You can ignore the result for fire-and-forget cancellation, or await it to confirm:

```clojure
(-> my-task
  (q/cancel)
  (q/ok #(if % (log "Task cancelled.") (log "Task already finished."))))
```

The returned task settles once the cancelled subtree is "quiescent": all tasks, subtasks, and `finally` handlers have
completed. Note that quiescence doesn't guarantee that all threads have been released; threads are responsible for
responding to interruption and may delay doing so.

A cancelled task contains a `CancellationException` and will throw if dereferenced.

#### `compel` and direct cancellation

As stated earlier, `compel` ignores cascade cancellation and allows a task to outlive the scope of its parent.

```clojure
(def parent
  (q/task
    (let [subtask (q/sleep 100 "I'm alive!")]
      (q/race (q/compel subtask) (q/sleep 50))
      @subtask)))
```

Here `race` cannot tear down `subtask` because it sees the `compel` moat. But cancelling `parent` directly would still
cancel `subtask`. The moat that protects `subtask` only exists within the `race`, not in the outer `let`.

If you hold a reference to a `compel`ed task, you can still cancel it explicitly.

#### Detecting cancellation

Use `cancelled?` to check if a task was cancelled:

```clojure
(q/cancelled? some-task)  ; => true or false
```

Inside a `finally` handler, check the third argument:

```clojure
(q/finally task
  (fn [_v _e c]
    (release-resource)
    (when c
      (log/info "Task was cancelled"))))
```

### Controlling parallelism

To limit how many tasks are executed in parallel, use a semaphore:

```clojure
(let [sem (q/semaphore 20)]
  @(q/qfor [n (range 1000)]
     (q/task
       (q/with-semaphore sem
         (Thread/sleep (+ 50 (rand-int 100)))
         (println "Thread" n "done.")
         n))))

;; Prints a lot of "Thread <n> done."
;; => [0 1 2 … 999]
```

`qfor` is nothing special; it expands to `(q/q (mapv …))`.

Note that you can't by default acquire a semaphore permit inside a platform thread. This would block the thread, which
this library discourages. Acquire the permit within a virtual thread, then start the platform thread.

```clojure
(let [sem (q/semaphore 4)]
  @(q/qfor [work-unit (get-work-units)]
     (q/task
       (q/with-semaphore sem
         @(q/cpu-task (cpu-bound-calculation work-unit))))))
```

### Scopes

Initially, Quiescent automatically propagated thread bindings to all tasks, but this turned out to be too expensive. A
simple no-op task had ~20µs in pure overhead, and most of it (95% or more) was the time it took to set thread bindings.

As of JDK 25, there's an alternative to `ThreadLocal`: `ScopedValue`. It fits quite well together with Clojure's
immutable data structures.

Tasks support automatic scoped value propagation using the [Scoped](https://github.com/multiplyco/scoped)
library, and also uses it for some internal bookkeeping. The concession is that you have to use the `ask` verb to
extract the currently bound value.

```clojure 
(require '[co.multiply.scoped :refer [ask scoping]])

(def ^:dynamic *name*)

(scoping [*name* "Alice"]
  (q/task
    (println "Hello," (ask *name*))
    (scoping [*name* "Bob"]
      (q/task
        (println "Hello," (ask *name*))))))

;; Prints:
;; Hello, Alice
;; Hello, Bob
```

## Performance and overhead

The floor for a task that does nothing and runs synchronously on the current thread (i.e. `(q/q nil)`) is **91ns** as
measured on an M1 using Criterium. This sounds impressive until you realize that this is 91ns longer than it takes to
run `nil`. Still, this establishes where we're starting from.

If you add in some coordination, for example:

```clojure
(q/for [n (range 1000)]
  (q/q n))
```

This comes out to **~536µs**, or about **~0.54µs** per task. If we disregard that the `mapv` to which `qfor` expands
will cost some portion of this, most of the overhead is due to there being a parent task, with subtasks in its scope.
So the tasks engage in coordination:

- Signalling to ensure that if one task throws, all uncompelled siblings are torn down immediately
- Tracking to ensure that uncompelled tasks in the scope of the parent don't exceed the lifetime of the same parent
- Grounding of all subtask values into the value of the parent (in this case, a vector)

Starting a virtual thread itself takes somewhere between **0.1µs** to **1µs**, so we can remeasure with this:

```clojure
(q/for [n (range 1000)]
  (q/task n)) ;; <- Run 1000 virtual threads
```

This now takes **849µs**, where the additional cost can be attributed to the cost of submitting the body of the task to
a virtual executor. So we spent somewehere around ~**0.3µs** per task starting a thread.

Let's measure some grounding:

```clojure
(q/q {:hello         (q/q :world)
      :animals       #{(q/q :dog) (q/q :cat) (q/q :capybara)}
      :frozen-places [(q/q "North pole") (q/q "My freezer")]})
```

This comes out to **3.7µs** or about **0.52µs** per task, on the same machine as earlier. This is not a case where we
have many tasks, so the overhead is now mostly due to the same coordination and grounding as mentioned above. And indeed
it's about the same cost per task (modulo noise) that we saw in the 1000 task example.

Dereferencing this would return:

```clojure
{:hello         :world
 :animals       #{:cat :capybara :dog}
 :frozen-places ["North pole" "My freezer"]}
```

You might not be using an M1, so the exact numbers here are not important. But understanding the relative ratio between
the cost of a task in isolation, and the cost of tasks under coordination, can be useful to help you reason about your
own workloads.

## Platform thread safety

By default, dereferencing a task on a platform thread throws an exception. This prevents accidentally blocking carrier
threads in virtual thread pools.

```clojure
; On a platform thread:
@my-task  ; => throws "Refusing to park platform thread"

; Explicitly allow it:
(q/deref-cpu my-task)  ; => blocks and returns result
```

Disable this check globally with `(q/throw-on-platform-park! false)`.

## Integration

### CompletableFuture

CompletableFutures integrate pretty much directly with tasks without special consideration.

```clojure
; Wrap a CompletableFuture as a task
(q/as-task (s3/async-get …)) ;; <- Presuming that `async-get` returns a CompletableFuture

;; Or just use it directly:
(q/qlet [some-cf (s3/async-get …)
         some-other (q/task …)]
  (str some-cf some-other))

(-> (s3/async-get …)
  (q/then …))

(-> (s3/async-get …)
  (q/timeout 100 (Exception. "Too slow!")))

; Convert task to CompletableFuture
(q/as-cf my-task)
```

### core.async (optional)

core.async support is available as an optional adapter. Add core.async to your own dependencies, then require the
adapter:

```clojure
(require '[co.multiply.quiescent.adapter.core-async :as q-async])

; Wrap a channel as a task (takes first value)
(q/as-task some-channel)

; Convert task to channel
(q-async/as-chan my-task)
```

## Examples

### Graceful Shutdown

When processing work that acquires external resources, cleanup must run even if the parent task is cancelled. Use
`compel` to protect critical cleanup from cancellation.

Resources should protect their own cleanup. By placing `compel` inside `close-conn`, safety is guaranteed wherever
it's used and callers don't need to remember to protect it ad-hoc.

```clojure
(require '[co.multiply.quiescent :as q])

(defn open-conn
  [id]
  (println "Opening connection" id))


(defn close-conn
  [id]
  ;; compel ensures cleanup completes even if parent is cancelled
  (q/compel
    (-> (q/task
          (Thread/sleep 1000)
          (println "Connection" id "released"))
      (q/finally
        (fn [& _]
          (println "Connection" id "leaked!"))))))


(defn process-with-resource
  [id]
  (open-conn id)
  (-> (q/task
        (println "Working on" id)
        (Thread/sleep 5000)
        (println "Finished" id))
    (q/finally
      (fn [_v e _c]
        (when e (println "Work" id "interrupted"))
        (close-conn id)))))


(q/throw-on-platform-park! false)

(def parent
  (q/task
    [(process-with-resource :a)
     (process-with-resource :b)]))

;; Cancel after 1 second - work stops, but connections still release:
(Thread/sleep 1000)
@(q/cancel parent)

;; Prints:
;; Opening connection :a
;; Opening connection :b
;; Working on :a
;; Working on :b
;; Work :a interrupted
;; Work :b interrupted
;; Connection :a released
;; Connection :b released
```

Try removing `compel` from `close-conn`. You'll see `Connection :a leaked!` instead of `Connection :a released` as the
cleanup gets cancelled along with everything else, and connections leak.

### Racing Structures

You can race not just individual tasks, but entire data structures. Grounding resolves all tasks within a structure,
so racing structures means "first group to fully complete wins."

```clojure
(require '[co.multiply.quiescent :as q])

(q/throw-on-platform-park! false)

(defn random-waiter
  [id]
  (let [ms (long (+ 10 (rand-int 50)))]
    (println (format "`%s` will wait %sms" id ms))
    (q/task (Thread/sleep ms)
      {id ms})))

(let [t1 (random-waiter :a)
      t2 (random-waiter :b)
      t3 (random-waiter :c)]
  (println "Winner combo:"
    @(q/race #{t1 t2} #{t2 t3} #{t1 t3})))

;; `:a` will wait 37ms
;; `:b` will wait 23ms
;; `:c` will wait 10ms
;; Winner combo: #{{:c 10} {:b 23}}
```

The pair containing the two fastest tasks wins. They execute in parallel, so the race is effectively decided by the
slowest task in the fastest group. Losing pairs are cancelled mid-flight.

Each task appears in multiple pairs. This is safe: cancelling an already-completed task is a no-op, and its
resolved value won't be overwritten with a cancellation exception. In this case, only `:a` was actually cancelled.

### Automatic Parallelization with qlet

`qlet` analyzes symbol dependencies and automatically parallelizes independent bindings. You don't specify what runs
in parallel. `qlet` instead infers it from the data flow.

```clojure
(require '[co.multiply.quiescent :as q])
(require '[co.multiply.scoped :refer [ask scoping]])


(def ^:dynamic *start-ms*)

(defn log [msg]
  (println (format "[%3dms] %s" (- (System/currentTimeMillis) (ask *start-ms*)) msg)))


(defn fetch-user [id]
  (q/task
    (log "Fetching user...")
    (Thread/sleep 100)
    (log "User fetched")
    {:id id :name "Alice"}))

(defn fetch-orders [user-id]
  (q/task
    (log (str "Fetching orders for user " user-id "..."))
    (Thread/sleep 150)
    (log "Orders fetched")
    [{:id 1} {:id 2}]))

(defn fetch-recommendations [user-id]
  (q/task
    (log (str "Fetching recs for user " user-id "..."))
    (Thread/sleep 120)
    (log "Recs fetched")
    [:product-a :product-b]))

(defn fetch-promotions []
  (q/task
    (log "Fetching promotions...")
    (Thread/sleep 80)
    (log "Promotions fetched")
    [:promo-1 :promo-2]))


(q/throw-on-platform-park! false)

(time
  (scoping [*start-ms* (System/currentTimeMillis)]
    @(q/qlet [user (fetch-user 123)
              orders (fetch-orders (:id user))          ; depends on user
              recs (fetch-recommendations (:id user)) ; depends on user
              promos (fetch-promotions)]                ; independent
       {:user (:name user) :orders orders :recs recs :promos promos})))

;; [  2ms] Fetching user...
;; [  2ms] Fetching promotions...
;; [ 86ms] Promotions fetched
;; [103ms] User fetched
;; [103ms] Fetching recs for user 123...
;; [103ms] Fetching orders for user 123...
;; [229ms] Recs fetched
;; [257ms] Orders fetched
;; "Elapsed time: 257ms"
;;
;; => {:user   "Alice"
;;     :orders [{:id 1} {:id 2}]
;;     :recs   [:product-a :product-b]
;;     :promos [:promo-1 :promo-2]}
```

Sequential execution would take 450ms (100+150+120+80). qlet takes 257ms because:

- `user` and `promos` run in parallel (both independent)
- `orders` and `recs` run in parallel (both depend only on `user`)

The `qlet` above expands to:

```clojure
(q/task
  (let [form-0 (fetch-user 123)
        form-1 (q/then form-0
                 (fn [param-0]
                   (let [user param-0]
                     (fetch-orders (:id user)))))
        form-2 (q/then form-0
                 (fn [param-0]
                   (let [user param-0]
                     (fetch-recommendations (:id user)))))
        form-3 (fetch-promotions)]
    (q/then form-0 form-1 form-2 form-3
      (fn [param-0 param-1 param-2 param-3]
        (let [user   param-0
              orders param-1
              recs   param-2
              promos param-3]
          {:user (:name user), :orders orders, :recs recs, :promos promos})))))
```

Note that forms with no dependencies remain unchanged and are not automatically wrapped with task conversion. This is
because:

- Much of the time, they will be functions that return tasks. It would be redundant.
- `then` accepts non-tasks as arguments anyway.

Supply your own `task`/`cpu-task` for forms where you truly want to construct a task inline.

### Happy Eyeballs

[Happy Eyeballs](https://en.wikipedia.org/wiki/Happy_Eyeballs) (RFC 8305) is a connection algorithm that improves
responsiveness by racing multiple connection attempts with staggered starts. Instead of trying addresses sequentially
(and waiting for each to timeout), it starts with the first address, then after a short delay begins the next attempt
while keeping the first running. Whichever connects first wins; the others are cancelled.

As inspired by [Missionary](https://github.com/leonoel/missionary/wiki/Happy-eyeballs).

```clojure
(require '[co.multiply.quiescent :as q])
(require '[co.multiply.scoped :refer [ask scoping]])
(import '[java.net InetAddress Socket])


(def ^:dynamic *start-ms*)


(defn log [msg]
  (println (format "[%3dms] %s" (- (System/currentTimeMillis) (ask *start-ms*)) msg)))


(defn connector
  [{:keys [addr port]}]
  (let [log-addr (str addr ":" port)]
    (log (str "Connecting: " log-addr))
    (-> (q/task (Socket. ^InetAddress addr (int port)))
      (q/finally
        (fn [_v e c]
          (cond
            c (log (str "Cancelled: " log-addr))
            e (log (str "Error: " log-addr))
            :else (log (str "Success: " log-addr))))))))


(defn happy-eyeballs
  [ms configs]
  (letfn [(attempt [[config & configs]]
            (if config
              (let [trigger (q/promise)]
                ;; `race-stateful` executes the given side effect on losers which
                ;; nevertheless had their value realized before they could be
                ;; cancelled.
                (q/race-stateful Socket/.close
                  ;; Attempt a connection with the given config.
                  (-> (connector config)
                    ;; If it fails immediately, deliver the remaining configs
                    ;; to the `trigger`. `err` is the side-effecting version
                    ;; of `catch`.
                    (q/err (fn [_e] (deliver trigger configs)))
                    ;; If we've waited `ms` milliseconds it's time to try
                    ;; another option. `monitor` is the side-effecting version
                    ;; of `timeout`.
                    (q/monitor ms #(deliver trigger configs)))
                  ;; When the promise `trigger` contains configs, follow up
                  ;; with a recursive call, the return value of which will
                  ;; be a participant in the race.
                  (q/then trigger attempt)))
              (throw (IllegalStateException. "No configs left to try."))))]
    ;; Wrap the initial attempt in a task so that the exception is swallowed
    ;; by the task if an empty `configs` vector is given to `happy-eyeballs`.
    (q/task (attempt configs))))


(def configs
  (into [{:addr (InetAddress/getByName "192.0.2.1") :port 80}
         {:addr "0" :port -1}]
    (mapv (fn [addr] {:addr addr :port 80})
      (InetAddress/getAllByName "clojure.org"))))


(q/throw-on-platform-park! false)

(time
  (scoping [*start-ms* (System/currentTimeMillis)]
    (let [socket @(happy-eyeballs 5 configs)]
      (log (str "Winner: " (.getInetAddress socket)))
      (Socket/.close socket)
      :success)))

;; Prints
;; [  0ms] Connecting: /192.0.2.1:80
;; [  6ms] Connecting: 0:-1
;; [  6ms] Error: 0:-1
;; [  7ms] Connecting: clojure.org/3.164.240.110:80
;; [ 13ms] Connecting: clojure.org/3.164.240.16:80
;; [ 15ms] Success: clojure.org/3.164.240.110:80
;; [ 15ms] Cancelled: clojure.org/3.164.240.16:80
;; [ 15ms] Cancelled: /192.0.2.1:80
;; [ 15ms] Winner: clojure.org/3.164.240.110
;; "Elapsed time: 15.34175 msecs"
```

## License

Eclipse Public License 2.0. Copyright (c) 2025 Multiply. See [LICENSE](LICENSE).

Authored by [@eneroth](https://github.com/eneroth)
