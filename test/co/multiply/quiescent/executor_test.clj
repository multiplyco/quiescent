(ns co.multiply.quiescent.executor-test
  "Context-classloader propagation for the executor thread factories (JVM only).

   Worker threads are constructed lazily on whatever thread submits the first task,
   so without an explicit context classloader they inherit the submitter's. That
   breaks lazy class generation on the worker (Specter dynamic-path eval, runtime
   reflection, agent fns) whenever the submitter is a thread whose loader can't
   resolve clojure.lang.* - e.g. a Rama module/daemon thread - surfacing as
   \"Syntax error compiling fn* ... ClassNotFoundException: clojure.lang.AFunction\".
   The factories pin the loader captured at namespace load instead."
  (:require
    [clojure.test :refer [deftest is testing]]
    [co.multiply.quiescent.impl.executor :as executor]))


(def ^:private submitters
  "Label -> a 1-arg fn that runs the given thunk on each executor's worker thread."
  {"q-io"  executor/delegate-virtual
   "q-cpu" executor/delegate-cpu
   "q-se"  #(executor/delegate-scheduled 0 %)})


(defn- on-worker
  "Run `(thunk)` on the worker thread reached via `submit!` and return its value.
   delegate-virtual/-cpu submit f as a Runnable (return value discarded), so the
   worker hands its value back through a promise."
  [submit! thunk]
  (let [p (promise)]
    (submit! (fn [] (deliver p (thunk))))
    (deref p 5000 ::timeout)))


(defn- worker-context-classloader
  [submit!]
  (on-worker submit! #(Thread/.getContextClassLoader (Thread/currentThread))))


(deftest worker-threads-carry-base-classloader
  (doseq [[label submit!] submitters]
    (testing label
      (is (identical? executor/base-classloader (worker-context-classloader submit!))
        "worker's context classloader should be the loader captured at executor load"))))


(deftest worker-classloader-ignores-submitter
  ;; The bug: a fresh worker inherits the submitting thread's context classloader.
  ;; Pin a distinctive bad loader on this thread and confirm workers don't pick it up.
  ;; (q-io creates a fresh virtual thread per task, so it exercises the override at
  ;; submit time; the pooled q-cpu/q-se threads are reused once created with base.)
  (let [original (Thread/.getContextClassLoader (Thread/currentThread))
        bad      (clojure.lang.DynamicClassLoader. nil)]
    (try
      (Thread/.setContextClassLoader (Thread/currentThread) bad)
      (doseq [[label submit!] submitters]
        (testing label
          (let [worker-cl (worker-context-classloader submit!)]
            (is (not (identical? bad worker-cl))
              "worker must not inherit the submitter's context classloader")
            (is (identical? executor/base-classloader worker-cl)
              "worker must carry the captured base classloader"))))
      (finally
        (Thread/.setContextClassLoader (Thread/currentThread) original)))))


(deftest worker-classloader-resolves-clojure-runtime
  ;; Deterministic stand-in for the production symptom. On a fresh worker Compiler/LOADER
  ;; is unbound, so the DynamicClassLoader that lazy class generation creates (Specter
  ;; dynamic-path eval, runtime reflection, agent fns) has the *context* classloader as
  ;; its parent; if that loader can't resolve clojure.lang.* the generation dies with
  ;; "ClassNotFoundException: clojure.lang.AFunction". We resolve that class through the
  ;; worker's context classloader directly, which reproduces exactly that lookup without
  ;; depending on ambient Compiler/LOADER state (kaocha happens to keep it bound, which
  ;; would otherwise mask the fall-through to the context loader).
  (let [original   (Thread/.getContextClassLoader (Thread/currentThread))
        ;; The platform classloader can't see clojure.lang.* - like a Rama daemon loader.
        restricted (ClassLoader/getPlatformClassLoader)]
    (try
      (Thread/.setContextClassLoader (Thread/currentThread) restricted)
      (doseq [[label submit!] submitters]
        (testing label
          (is (= :ok (on-worker submit!
                       (fn []
                         (try (Class/forName "clojure.lang.AFunction" false
                                (Thread/.getContextClassLoader (Thread/currentThread)))
                           :ok
                           (catch Throwable e (.getName (class e)))))))
            "worker's context classloader must resolve clojure.lang.* despite a restricted submitter")))
      (finally
        (Thread/.setContextClassLoader (Thread/currentThread) original)))))
