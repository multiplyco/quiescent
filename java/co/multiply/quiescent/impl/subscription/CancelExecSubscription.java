package co.multiply.quiescent.impl.subscription;

import clojure.lang.Keyword;
import co.multiply.quiescent.impl.TaskState;
import java.util.concurrent.Future;

/**
 * Cancels a running Future when the owning task reaches the
 * subscription's phase, but only if the task was cancelled.
 */
public final class CancelExecSubscription extends Subscription {
    private final Future<?> future;

    public CancelExecSubscription(Keyword phase, Future<?> future) {
        super(phase);
        this.future = future;
    }

    @Override
    public void runSub(TaskState taskState) {
        if (taskState.cancelled) {
            future.cancel(true);
        }
    }
}
