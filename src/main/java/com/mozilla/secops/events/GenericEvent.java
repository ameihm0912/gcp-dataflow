package com.mozilla.secops.events;

import com.google.api.services.logging.v2.model.LogEntry;

import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.lang.IllegalArgumentException;
import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;

public class GenericEvent implements Serializable {
    public enum Type {
        AUTH,
        GENERIC
    }

    public enum TimestampFormat {
        SYSLOG_RFC3164
    }

    private final Map<TimestampFormat, String> tsFormatMap;

    private UUID eventId;
    private String payload;
    private String hostname;
    private String program;
    private Detail<? extends Detail> detail;
    private Instant timestamp;

    @Override
    public boolean equals(Object o) {
        GenericEvent e = (GenericEvent)o;
        return getEventId().equals(e.getEventId());
    }

    GenericEvent() {
        eventId = UUID.randomUUID();
        tsFormatMap = new HashMap<TimestampFormat, String>();
        tsFormatMap.put(TimestampFormat.SYSLOG_RFC3164,
                "MMM dd HH:mm:ss");
        timestamp = Instant.now();
    }

    public void setPayload(String p) {
        payload = p;
    }
    
    public void setProgram(String p) {
        program = p;
    }

    public void setHostname(String h) {
        hostname = h;
    }

    public void setDetail(Detail<?> d) {
        detail = d;
    }

    public void setTimestampFromString(String tstr, TimestampFormat fmt)
            throws IllegalArgumentException {
        String f = tsFormatMap.get(fmt);
        if (f == null) {
            throw new IllegalArgumentException("invalid timestamp format");
        }
        DateTimeFormatter dfmt = DateTimeFormat.forPattern(f);
        timestamp = dfmt.parseDateTime(tstr).toInstant();
    }

    public Detail.Type getDetailType() {
        return detail.getType();
    }

    @SuppressWarnings("unchecked")
    public <T extends Detail> T getDetail() {
        return (T)detail;
    }

    public String getEventId() {
        return eventId.toString();
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getProgram() {
        return program;
    }

    public String getHostname() {
        return hostname;
    }

    public String getPayload() {
        return payload;
    }
}
