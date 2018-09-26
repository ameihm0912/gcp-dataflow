package com.mozilla.secops.events;

public abstract class Detail<T extends Detail> {
    public enum Type {
        AUTH,
        GENERIC
    }

    private T data;
    private Type type;

    public Type getType() {
        return type;
    }

    protected void setType(Type t) {
        type = t;
    }

    public void setDetail(T d) {
        data = d;
    }
}
