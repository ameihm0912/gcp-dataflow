package com.mozilla.secops;

import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.logging.v2.model.LogEntry;

import java.lang.IllegalArgumentException;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventProcessor implements Serializable {
    private final String syslogRe = "^(\\S{3} \\d{2} [\\d:]+) (\\S+) (\\S+)\\[\\d+\\]: .+";
    private final String authSSHRe = "^.*sshd\\[\\d+\\]: Accepted (\\S+) for (\\S+) from (\\S+) " +
        "port (\\d+).*";

    private Pattern syslogRePattern;
    private Pattern authSSHRePattern;

    EventProcessor() {
        syslogRePattern = Pattern.compile(syslogRe);
        authSSHRePattern = Pattern.compile(authSSHRe);
    }

    private String stripEncapsulation(GenericEvent g, String in) {
        try {
            JacksonFactory jf = new JacksonFactory();
            JsonParser jp = jf.createJsonParser(in);
            LogEntry entry = jp.parse(LogEntry.class);
            String ret = entry.getTextPayload();
            if (ret != null && !ret.isEmpty()) {
                return entry.getTextPayload();
            }
        } catch (java.io.IOException e) { }
        return in;
    }

    private Boolean tryAuthParseSSH(GenericEvent g, String raw) {
        Matcher mat = authSSHRePattern.matcher(raw);
        if (!mat.matches()) {
            return false;
        }
        AuthDetail ad = new AuthDetail();
        ad.fromStandardAttributes(mat.group(3), mat.group(2), g.getHostname());
        ad.setAuthType(AuthDetail.Type.SSH);
        g.setDetail(ad);
        return true;
    }

    private Boolean tryParseSyslog(GenericEvent g, String raw) {
        Matcher mat = syslogRePattern.matcher(raw);
        if (!mat.matches()) {
            return false;
        }
        try {
            g.setTimestampFromString(mat.group(1), GenericEvent.TimestampFormat.SYSLOG_RFC3164);
        } catch (IllegalArgumentException e) { }
        g.setHostname(mat.group(2));
        g.setProgram(mat.group(3));
        if (tryAuthParseSSH(g, raw)) {
            return true;
        }
        return false;
    }

    public GenericEvent parse(String event) {
        GenericEvent g = new GenericEvent();
        g.setPayload(event);

        String raw = stripEncapsulation(g, event);

        if (tryParseSyslog(g, raw)) {
            return g;
        }

        g.setDetail(new GenericDetail(raw));
        return g;
    }
}
