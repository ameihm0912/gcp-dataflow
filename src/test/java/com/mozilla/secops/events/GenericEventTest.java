package com.mozilla.secops.events;

import junit.framework.TestCase;

public class GenericEventTest extends TestCase {
    public GenericEventTest(String name) {
        super(name);
    }

    public void testGenericConstruct() throws Exception {
        GenericEvent e = new GenericEvent();
        assertNotNull(e);
    }
}
