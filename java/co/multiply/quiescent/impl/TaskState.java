package co.multiply.quiescent.impl;

public final class TaskState {
    public final boolean exceptional;
    public final boolean cancelled;
    public final Object result;

    public TaskState(boolean exceptional, boolean cancelled, Object result) {
        this.exceptional = exceptional;
        this.cancelled = cancelled;
        this.result = result;
    }
}
