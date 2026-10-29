package co.multiply.quiescent.impl.channel;

class TakeNode extends Node {
    final BoundedChannel ch;

    TakeNode(Thread thread, BoundedChannel ch) {
        super(thread);
        this.ch = ch;
    }

    Object execute() {
        boolean interrupted = false;
        while (true) {
            if (ch.isCancelled()) {
                this.release();
                if (interrupted) Thread.currentThread().interrupt();
                return IChannel.CANCELLED;
            }
            long h = ch.head;
            long t = (long) BoundedChannel.TAIL.getAcquire(ch);
            if (t - h > 0) {
                Object ownValue = ch.readAndClearSlot(h);

                ch.takeHead = h + 1;
                ch.takeTail = t;

                Node last = combineChain(this);

                ch.commitHeadAndSignal(ch.takeHead, h, last);

                if (interrupted) Thread.currentThread().interrupt();
                return ownValue;
            }
            if (ch.isSealed()) {
                this.release();
                if (interrupted) Thread.currentThread().interrupt();
                return IChannel.CANCELLED;
            }
            if (ch.takeLock.suspend()) {
                interrupted = true;
            }
        }
    }

    @Override
    boolean tryCombine() {
        if (ch.takeHead >= ch.takeTail || ch.isCancelled()) return false;
        this.value = ch.readAndClearSlot(ch.takeHead);
        ch.takeHead++;
        return true;
    }
}
