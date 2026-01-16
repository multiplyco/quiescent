package co.multiply.quiescent.impl;

import clojure.lang.Keyword;

public interface IStateful {
    boolean isExceptional();
    boolean isCancelled();
    boolean isCompelled();
    Keyword getPhase();
    Object getResult();
    Object getSubscriptions();
    Object getScope();
    void setScope(Object newScope);
    boolean atOrPast(Keyword phase);
}
