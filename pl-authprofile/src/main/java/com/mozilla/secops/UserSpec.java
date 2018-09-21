package com.mozilla.secops;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.io.InputStream;

import java.util.*;

class UserSpecRoot {
    public Map<String, UserSpecRootEntity> identities;
}

class UserSpecRootEntity {
    public List<String> aliases;
}

public class UserSpec {
    private UserSpecRoot specroot;

    UserSpec(InputStream is) throws java.io.IOException {
        ObjectMapper mapper = new ObjectMapper();
        specroot = mapper.readValue(is, UserSpecRoot.class);
    }
}
