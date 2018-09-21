package com.mozilla.secops;

import java.io.Serializable;

public class AuthDetail extends Detail implements Serializable {
    public enum Type {
        SSH
    }

    private Type type;
    private String sourceAddress;
    private String user;
    private String object;

    AuthDetail() {
        setType(Detail.Type.AUTH);
    }

    public String getUser() {
        return user;
    }

    public String getSourceAddress() {
        return sourceAddress;
    }

    public String getObject() {
        return object;
    }

    public Type getAuthType() {
        return type;
    }

    public void fromStandardAttributes(String sa, String u, String o) {
        sourceAddress = sa;
        user = u;
        object = o;
    }

    public void setAuthType(Type t) {
        type = t;
    }
}
