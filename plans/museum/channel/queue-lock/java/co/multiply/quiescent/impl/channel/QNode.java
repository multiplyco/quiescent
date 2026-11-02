package co.multiply.quiescent.impl.channel;

/**
 * Intrusive queue node for the channel queue lock.
 * Carries the thread to wake and a next pointer.
 */
public final class QNode {
    volatile Thread thread;
    volatile QNode next;

    QNode(Thread thread) {
        this.thread = thread;
    }
}
