package com.mozilla.secops.parser;

import java.io.Serializable;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class OpenSSH extends Payload implements Serializable {
    private final String matchRe = "^\\S{3} \\d{2} [\\d:]+ \\S+ \\S*sshd\\[\\d+\\]: .+";
    private Pattern pattRe;

    private final String authAcceptedRe = "^.*sshd\\[\\d+\\]: Accepted (\\S+) for (\\S+) from (\\S+) " +
        "port (\\d+).*";
    private Pattern pattAuthAcceptedRe;

    private String user;
    private String authMethod;
    private String sourceAddress;

    @Override
    public Boolean matcher(String input) {
        Matcher mat = pattRe.matcher(input);
        if (mat.matches()) {
            return true;
        }
        return false;
    }

    public OpenSSH() {
        pattRe = Pattern.compile(matchRe);
    }

    public OpenSSH(String input) {
        pattAuthAcceptedRe = Pattern.compile(authAcceptedRe);

        setType(Payload.PayloadType.OPENSSH);

        Matcher mat = pattAuthAcceptedRe.matcher(input);
        if (mat.matches()) {
            authMethod = mat.group(1);
            user = mat.group(2);
            sourceAddress = mat.group(3);
        }
    }

    public String getUser() {
        return user;
    }

    public String getAuthMethod() {
        return authMethod;
    }

    public String getSourceAddress() {
        return sourceAddress;
    }
}
