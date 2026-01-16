package co.multiply.quiescent.impl.subscription;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import co.multiply.quiescent.impl.TaskState;

/**
 * Invokes an arbitrary function with the task state when the owning
 * task reaches the subscription's phase.
 */
public final class CallbackSubscription extends Subscription {
    private final IFn f;

    public CallbackSubscription(Keyword phase, IFn f) {
        super(phase);
        this.f = f;
    }

    @Override
    public void runSub(TaskState taskState) {
        try {
            f.invoke(taskState);
        } catch (Throwable e) {
            throw new IllegalStateException("Subscription threw during task lifecycle.", e);
        }
    }
}
