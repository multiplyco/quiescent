package co.multiply.quiescent.impl.channel;

import clojure.lang.RT;

class PutNode extends Node {
    final BoundedChannel ch;

    PutNode(Thread thread, BoundedChannel ch) {
        super(thread);
        this.ch = ch;
    }

    boolean putAccumulate(Object value) {
        while (true) {
            if (!ch.isOpen()) return false;
            if (ch.hasCapacity(ch.putTail)) {
                ch.writeSlot(ch.putTail, value);
                ch.putTail++;
                return true;
            }
            // Preserve baseline volatile publication in the node-only experiment.
            BoundedChannel.TAIL.setVolatile(ch, ch.putTail);
            ch.takeLock.resume();
            if (ch.hasCapacity(ch.putTail)) continue;
            if (ch.putLock.suspend()) {
                ch.putInterrupted = true;
            }
        }
    }

    boolean writeValue(Object value) {
        return ch.rf != null ? putXf(value) : putAccumulate(value);
    }

    boolean putXf(Object value) {
        if (!ch.isOpen()) return false;
        Object result = ch.rf.invoke(ch, value);
        if (RT.isReduced(result)) {
            ch.rf.invoke(ch);
            ch.putSealed = true;
            return true;
        }
        return ch.isOpen();
    }

    boolean execute() {
        ch.putInterrupted = false;
        ch.putSealed = false;
        ch.putTail = ch.tail;

        boolean ok = writeValue(this.value);

        Node last = this;
        if (ok && !ch.putSealed) {
            last = combineChain(this);
        }

        boolean flushed = ch.putTail != ch.tail;
        if (flushed) {
            // Preserve baseline volatile publication in the node-only experiment.
            BoundedChannel.TAIL.setVolatile(ch, ch.putTail);
        }

        if (ch.putSealed) ch.seal();

        last.release();

        if (flushed && !ch.putSealed) {
            ch.takeLock.resume();
        }

        if (ch.putInterrupted) Thread.currentThread().interrupt();
        return ok;
    }

    @Override
    boolean tryCombine() {
        if (!ch.isOpen() || ch.putSealed) return false;
        return writeValue(this.value);
    }
}
