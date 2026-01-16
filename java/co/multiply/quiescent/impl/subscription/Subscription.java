package co.multiply.quiescent.impl.subscription;

import clojure.lang.Keyword;
import co.multiply.quiescent.impl.TaskState;
import co.multiply.conc.Link;

/**
 * Abstract base for all subscription types.
 *
 * A Subscription is a {@link Link} in a task's subscription deque.
 * Each subscription fires at a specific lifecycle phase and performs
 * a type-specific action via {@link #runSub(TaskState)}.
 */
public abstract class Subscription extends Link {
    public final Keyword phase;

    protected Subscription(Keyword phase) {
        this.phase = phase;
    }

    public Keyword getPhase() {
        return phase;
    }

    public abstract void runSub(TaskState taskState);
}
