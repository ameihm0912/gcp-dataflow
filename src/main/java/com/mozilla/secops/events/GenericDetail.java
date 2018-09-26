package com.mozilla.secops.events;

import java.io.Serializable;

public class GenericDetail extends Detail implements Serializable {
    private String raw;

    GenericDetail(String s) {
        raw = s;
        setType(Detail.Type.GENERIC);
    }

    public String getRaw() {
        return raw;
    }
}
