package co.multiply.quiescent.impl.channel;

/**
 * Intrusive queue node for the channel queue lock.
 * Carries the thread to wake, a next pointer, and the value
 * for producer nodes.
 */
public final class QNode {
    volatile Thread thread;
    volatile QNode next;
    Object value;

    QNode(Thread thread, Object value) {
        this.thread = thread;
        this.value = value;
    }
}
