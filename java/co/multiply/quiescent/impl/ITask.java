package co.multiply.quiescent.impl;

import clojure.lang.Keyword;
import java.time.Duration;

public interface ITask {
    // Query
    Object getNow(Object defaultValue);
    Object newQuiescenceProxy(Object defaultValue);

    // Blocking await (CLJ only - not available in CLJS)
    boolean awaitPhase(Keyword phase);
    boolean awaitPhaseMillis(Keyword phase, long timeoutMs);
    boolean awaitPhaseDur(Keyword phase, Duration timeout);

    // Entry points
    boolean doRun(Object f, Object tf);

    // Phases
    boolean doGround(Object v, Object tf);
    boolean doTransform(Object res, Object tf);
    boolean doWrite(TaskState taskState);
    boolean doSettle();
    boolean doQuiesce();

    // Subscription
    Object doSubscribe(Object sub);
    boolean doUnsubscribe(Object sub);
    void runSubscriptions();

    // Chain transformations
    Object doThen(Object delegator, Object f);
    Object doHandle(Object delegator, Object f);
    Object doCatch(Object delegator, Object f);
    Object doCatchTyped(Object delegator, Object typeHandlerPairs);

    // Chain side-effects
    Object doOk(Object delegator, Object f);
    Object doErr(Object delegator, Object f);
    Object doDone(Object delegator, Object f);

    // Chain teardown/cleanup
    Object doFinally(Object delegator, Object f);
}
