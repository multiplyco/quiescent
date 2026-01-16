package co.multiply.quiescent.impl;

public interface ICancellable {
    boolean doCancel(String msg);
    ITask doCancelDirect();
    boolean doCancelCascade();
}
