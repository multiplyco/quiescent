package co.multiply.quiescent.impl.subscription;

import clojure.lang.Keyword;
import co.multiply.quiescent.impl.ICancellable;
import co.multiply.quiescent.impl.TaskState;

/**
 * Cascading-cancels a target task when the owning task reaches the
 * subscription's phase.
 */
public final class TeardownSubscription extends Subscription {
    private final ICancellable target;

    public TeardownSubscription(Keyword phase, ICancellable target) {
        super(phase);
        this.target = target;
    }

    @Override
    public void runSub(TaskState taskState) {
        target.doCancelCascade();
    }
}
