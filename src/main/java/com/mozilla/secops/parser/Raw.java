package com.mozilla.secops.parser;

public class Raw extends Payload {
    private String raw;

    @Override
    public Boolean matcher(String input) {
        return true;
    }

    public Raw() {
    }

    public Raw(String input) {
        setType(Payload.PayloadType.RAW);
        raw = input;
    }

    public String getRaw() {
        return raw;
    }
}
