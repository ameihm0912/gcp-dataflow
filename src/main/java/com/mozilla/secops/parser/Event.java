package com.mozilla.secops.parser;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.UUID;

public class Event {
    private Payload<? extends Payload> payload;
    private UUID eventId;
    private DateTime timestamp;

    Event() {
        eventId = UUID.randomUUID();

        // Default the event timestamp to creation time
        timestamp = new DateTime(DateTimeZone.UTC);
    }

    public void setPayload(Payload<?> p) {
        payload = p;
    }

    @SuppressWarnings("unchecked")
    public <T extends Payload> T getPayload() {
        return (T)payload;
    }

    public Payload.PayloadType getPayloadType() {
        return payload.getType();
    }

    public UUID getEventId() {
        return eventId;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }
}
