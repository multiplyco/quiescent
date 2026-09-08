(ns co.multiply.quiescent.gate-soundness-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [co.multiply.conc.queue :as queue]
    [co.multiply.quiescent :as q]
    [co.multiply.quiescent.impl.executor :as executor]
    [co.multiply.quiescent.test-support :refer [allow-platform-park]]
    [co.multiply.quiescent.type.call :as call]
    [co.multiply.scoped :refer [current-scope with-scope]]))


(allow-platform-park)


(def ^:private variants
  {:queued    (fn [g f] (q/gate-task g (f)))
   :immediate (fn [g f] (q/try-gate-task g (f)))})


(deftest nested-gates-test
  ;; Execute inline so a deadlock is observable as a pending result, without
  ;; hanging the test runner or relying on a timeout.
  (with-redefs [executor/delegate-virtual executor/delegate-sync]
    (doseq [[outer-name outer] variants
            [middle-name middle] variants
            [inner-name inner] variants]
      (testing (str [outer-name middle-name inner-name])
        (let [a (q/gate 1)
              b (q/gate 1)
              t (outer a #(middle b (fn [] (inner a (constantly :done)))))]
          (try
            (is (= :done (q/get-now t ::pending)))
            (finally
              (call/doCancel t "Test cleanup."))))))))


(deftest detached-reentrant-task-retains-permit-test
  (with-redefs [executor/delegate-virtual executor/delegate-sync]
    (doseq [[outer-name outer] variants
            [inner-name inner] variants]
      (testing (str [outer-name inner-name])
        (let [g      (q/gate 1)
              finish (q/promise)
              child  (atom nil)
              parent (outer g
                       #(do
                          (reset! child (q/compel (inner g (constantly finish))))
                          :parent))]
          (is (= :parent (q/get-now parent ::pending)))
          (is (q/cancelled? (q/try-gate-task g :too-soon))
            "A detached nested task must not lose its permit when its creator settles")
          (finish :child)
          (is (= :child (q/get-now @child ::pending)))
          (is (= :next (q/get-now (q/try-gate-task g :next) ::pending))))))))


(deftest queued-task-keeps-its-own-gate-scope-test
  (with-redefs [executor/delegate-virtual executor/delegate-sync]
    (let [a      (q/gate 1)
          b      (q/gate 1)
          finish (q/promise)
          _head  (q/gate-task b finish)
          t      (q/gate-task a (q/gate-task b (q/try-gate-task a :done)))]
      (finish :head)
      (is (= :done (q/get-now t ::pending)))
      (call/doCancel t "Test cleanup."))
    (let [a        (q/gate 1)
          b        (q/gate 1)
          finish-a (q/promise)
          finish-b (q/promise)
          parent   (q/gate-task a [(q/gate-task b finish-b) finish-a])
          outsider (q/gate-task b (q/try-gate-task a :must-not-borrow))]
      (finish-b :done)
      (is (q/cancelled? outsider)
        "The drainer must not lend its own enclosing grants to an unrelated queued task")
      (finish-a :done)
      (is (= [:done :done] (q/get-now parent ::pending))))))


(deftest completed-borrower-does-not-release-live-parent-test
  (with-redefs [executor/delegate-virtual executor/delegate-sync]
    (doseq [[_ outer] variants
            [_ inner] variants]
      (let [g      (q/gate 1)
            finish (q/promise)
            parent (outer g #(vector (inner g (constantly :inner)) finish))]
        (is (q/cancelled? (q/try-gate-task g :too-soon)))
        (finish :parent)
        (is (= [:inner :parent] (q/get-now parent ::pending)))
        (is (= :next (q/get-now (q/try-gate-task g :next) ::pending)))))))


(deftest surviving-borrower-can-still-reenter-test
  (with-redefs [executor/delegate-virtual executor/delegate-sync]
    (doseq [[_ submit] variants]
      (let [g      (q/gate 1)
            ready  (q/promise)
            child  (atom nil)
            parent (submit g
                     #(do
                        (reset! child
                          (q/compel
                            (submit g
                              (fn []
                                (q/then ready
                                  (fn [_] (submit g (constantly :child))))))))
                        :parent))]
        (is (= :parent (q/get-now parent ::pending)))
        (ready :go)
        (is (= :child (q/get-now @child ::pending)))
        (is (= :next (q/get-now (q/try-gate-task g :next) ::pending)))))))


(deftest cancelled-before-dispatch-test
  (doseq [[variant submit] variants]
    (testing (name variant)
      (let [scheduled (atom [])
            ran       (atom [])
            g         (q/gate 1)]
        (with-redefs [executor/delegate-virtual (fn [f] (swap! scheduled conj f) nil)]
          (let [t (submit g #(swap! ran conj :cancelled))]
            (call/doCancel t "Cancelled before executor dispatch.")
            (let [next (q/try-gate-task g :next)]
              ;; Simulate a queued microtask, which cannot be removed once
              ;; scheduled, or a JVM executor racing its cancellation handle.
              (run! (fn [f] (f)) @scheduled)
              (is (q/cancelled? t))
              (is (empty? @ran))
              (is (= :next (q/get-now next ::pending))))))))))


(deftest already-cancelled-at-admission-test
  (with-redefs [executor/delegate-virtual executor/delegate-sync]
    (doseq [[variant submit] variants]
      (testing (name variant)
        (let [g      (q/gate 1)
              ran    (atom false)
              parent (q/task :settled)
              ;; Re-enter a settled creator's scope, making registration
              ;; cancel the new task before admission gets control back.
              t      (with-scope (call/getScope parent)
                       (submit g #(reset! ran true)))
              finish (q/promise)
              next   (q/try-gate-task g finish)]
          (is (q/cancelled? t))
          (is (false? @ran))
          (is (not (q/cancelled? next)))
          (is (q/cancelled? (q/try-gate-task g :overflow)))
          (finish :next)
          (is (= :next (q/get-now next ::pending))))))))


(deftest borrower-settlement-releases-last-share-test
  (with-redefs [executor/delegate-virtual executor/delegate-sync]
    (doseq [[outer-name outer] variants
            [inner-name inner] variants
            outcome [:success :failure :cancel]]
      (testing (str [outer-name inner-name outcome])
        (let [g      (q/gate 1)
              finish (q/promise)
              child  (atom nil)
              parent (outer g
                       #(do
                          (reset! child
                            (q/compel
                              (inner g
                                (fn []
                                  (q/then finish
                                    (fn [_]
                                      (if (= outcome :failure)
                                        (throw (ex-info "Borrower failed." {}))
                                        :done)))))))
                          :parent))
              queued (q/gate-task g :queued)]
          (is (= :parent (q/get-now parent ::pending)))
          (is (not (realized? queued)))
          (if (= outcome :cancel)
            (call/doCancel @child "Cancel borrower.")
            (finish :done))
          (is (realized? @child))
          (is (= (= outcome :cancel) (q/cancelled? @child)))
          (is (= (not= outcome :success) (q/exceptional? @child)))
          (is (= :queued (q/get-now queued ::pending)))
          (call/doCancel @child "Repeated cancellation must not return another permit.")
          (let [hold (q/promise)
                next (q/try-gate-task g hold)]
            (is (not (q/cancelled? next)))
            (is (q/cancelled? (q/try-gate-task g :overflow)))
            (hold :next)))))))


(deftest fifo-survives-queued-cancellation-test
  (with-redefs [executor/delegate-virtual executor/delegate-sync]
    (let [g      (q/gate 1)
          finish (q/promise)
          head   (q/gate-task g finish)
          order  (atom [])
          tasks  (mapv (fn [n] (q/gate-task g (swap! order conj n))) (range 6))]
      (doseq [n [1 3]]
        (call/doCancel (nth tasks n) "Cancelled while queued."))
      (is (q/cancelled? (q/try-gate-task g (swap! order conj :barged))))
      (finish :head)
      (is (= :head (q/get-now head ::pending)))
      (is (= [0 2 4 5] @order))
      (is (= 6 (count (filter realized? tasks)))))))


(deftest dispatch-failure-releases-permit-test
  (doseq [[variant submit] variants]
    (testing (name variant)
      (let [reject? (atom true)
            failure (ex-info "Executor rejected dispatch." {})
            g       (q/gate 1)]
        (with-redefs [executor/delegate-virtual
                      (fn [f]
                        (if @reject?
                          (throw failure)
                          (executor/delegate-sync f)))]
          (let [t (submit g (constantly :never))]
            (is (identical? failure (q/get-ex t)))
            (is (not (q/cancelled? t)))
            (reset! reject? false)
            (is (= :next (q/get-now (q/try-gate-task g :next) ::pending)))))))))


(deftest synchronous-handoff-does-not-recurse-test
  (with-redefs [executor/delegate-virtual executor/delegate-sync]
    (let [g      (q/gate 1)
          finish (q/promise)
          head   (q/gate-task g finish)
          ran    (atom 0)
          tasks  (mapv (fn [_] (q/gate-task g (swap! ran inc))) (range 10000))]
      (finish :head)
      (is (= :head (q/get-now head ::pending)))
      (is (= 10000 @ran))
      (is (= 10000 (count (filter realized? tasks))))
      (is (= :next (q/get-now (q/try-gate-task g :next) ::pending))))))


(deftest cancelled-queue-entries-are-reclaimed-test
  (let [g     (q/gate 0)
        tasks (mapv (fn [_] (q/gate-task g :never)) (range 100))]
    (run! #(call/doCancel % "Cancelled while queued.") tasks)
    (is (every? q/cancelled? tasks))
    (is (queue/isEmpty (.-queue ^co.multiply.quiescent.impl.gate.Gate g))
      "Cancelled bodies must not remain retained by a long-lived gate")))


(deftest gate-capacity-validation-test
  (doseq [n [-1 1.5 nil :invalid "1" 2147483648 ##NaN ##Inf]]
    (testing (str n)
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core/ExceptionInfo)
            (q/gate n)))))
  (is (some? (q/gate 0)))
  (is (some? (q/gate 2147483647))))


#?(:clj
   (deftest reentrant-admission-races-holder-settlement-test
     (dotimes [_ 25]
       (let [g       (q/gate 1)
             ready   (clojure.core/promise)
             finish  (q/promise)
             holder  (q/gate-task g (deliver ready (current-scope)) finish)
             scope   (deref ready 2000 ::timeout)
             start   (java.util.concurrent.CountDownLatch. 1)
             holds   (vec (repeatedly 32 q/promise))
             tasks   (object-array 32)
             threads (mapv (fn [n]
                             (Thread.
                               (fn []
                                 (.await start)
                                 (with-scope scope
                                   (doseq [i (range n 32 4)]
                                     (aset tasks i
                                       (q/compel (q/try-gate-task g (nth holds i)))))))))
                       (range 4))]
         (is (not= ::timeout scope))
         (run! #(Thread/.start ^Thread %) threads)
         (.countDown start)
         (call/doCancel holder "Race the last release against reentrant admission.")
         (run! #(Thread/.join ^Thread % 2000) threads)
         (is (not-any? #(Thread/.isAlive ^Thread %) threads))
         (is (pos? (count (remove q/cancelled? tasks))))
         (is (q/cancelled? (q/try-gate-task g :must-remain-busy)))
         (run! (fn [hold] (hold :done)) holds)
         (finish :done)
         (is (every? true? (mapv #(deref (q/await % 2000)) tasks)))
         ;; Once all borrowers settle, recover exactly one permit.
         (let [hold (q/promise)
               next (q/try-gate-task g hold)]
           (is (not (q/cancelled? next)))
           (is (q/cancelled? (q/try-gate-task g :overflow)))
           (hold :next)
           (is (true? @(q/await next 2000))))))))
