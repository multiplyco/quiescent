package co.multiply.quiescent.impl.subscription;

import clojure.lang.Keyword;
import co.multiply.quiescent.impl.ITask;
import co.multiply.quiescent.impl.TaskState;

/**
 * Removes a subscription from a target task when the owning task
 * reaches the subscription's phase. Used for cross-task cleanup
 * (e.g., removing a teardown subscription when the target settles
 * before the owner).
 */
public final class UnsubSubscription extends Subscription {
    private final ITask target;
    private final Subscription sub;

    public UnsubSubscription(Keyword phase, ITask target, Subscription sub) {
        super(phase);
        this.target = target;
        this.sub = sub;
    }

    @Override
    public void runSub(TaskState taskState) {
        target.doUnsubscribe(sub);
    }
}
