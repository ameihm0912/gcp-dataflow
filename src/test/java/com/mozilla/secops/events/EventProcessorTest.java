package com.mozilla.secops.events;

import junit.framework.TestCase;

public class EventProcessorTest extends TestCase {
    public EventProcessorTest(String name) {
        super(name);
    }

    public void testParseUnknown() throws Exception {
        GenericEvent e = new EventProcessor().parse("unknown event type");
        assertNotNull(e);
        assertEquals("unknown event type", e.getPayload());
        assertEquals(Detail.Type.GENERIC, e.getDetailType());
        GenericDetail gd = e.getDetail();
        assertEquals("unknown event type", gd.getRaw());
    }

    public void testStripStackdriver() throws Exception {
        String buf = "{\"insertId\":\"f8p4mz1a3ldcos1xz\",\"labels\":{\"compute.googleapis.com/resource" +
            "_name\":\"emit-bastion\"},\"logName\":\"projects/sandbox-00/logs/syslog\",\"receiveTimestamp\"" +
            ":\"2018-09-20T18:43:38.318580313Z\",\"resource\":{\"labels\":{\"instance_id\":\"99999999999999" +
            "99999\",\"project_id\":\"sandbox-00\",\"zone\":\"us-east1-b\"},\"type\":\"gce_instance\"},\"te" +
            "xtPayload\":\"Sep 18 22:15:38 emit-bastion log[2644]: logdata\"" +
            ",\"timestamp\":\"2018-09-18T22:15:38Z\"}";
        String expectRaw = "Sep 18 22:15:38 emit-bastion log[2644]: logdata";

        GenericEvent e = new EventProcessor().parse(buf);
        assertNotNull(e);
        assertEquals(Detail.Type.GENERIC, e.getDetailType());
        GenericDetail gd = e.getDetail();
        assertEquals(expectRaw, gd.getRaw());
    }

    public void testStripStackdriverNoTextPayload() throws Exception {
        String buf = "{\"insertId\":\"f8p4mz1a3ldcos1xz\",\"labels\":{\"compute.googleapis.com/resource" +
            "_name\":\"emit-bastion\"},\"logName\":\"projects/sandbox-00/logs/syslog\",\"receiveTimestamp\"" +
            ":\"2018-09-20T18:43:38.318580313Z\",\"resource\":{\"labels\":{\"instance_id\":\"99999999999999" +
            "99999\",\"project_id\":\"sandbox-00\",\"zone\":\"us-east1-b\"},\"type\":\"gce_instance\"}," +
            "\"timestamp\":\"2018-09-18T22:15:38Z\"}";

        GenericEvent e = new EventProcessor().parse(buf);
        assertNotNull(e);
        assertEquals(buf, e.getPayload());
        assertEquals(Detail.Type.GENERIC, e.getDetailType());
        GenericDetail gd = e.getDetail();
        assertEquals(buf, gd.getRaw());
    }

    public void testProcessSSH() throws Exception {
        String buf = "{\"insertId\":\"f8p4mz1a3ldcos1xz\",\"labels\":{\"compute.googleapis.com/resource_" +
            "name\":\"emit-bastion\"},\"logName\":\"projects/sandbox-00/logs/syslog\",\"receiveTimestamp\"" +
            ":\"2018-09-20T18:43:38.318580313Z\",\"resource\":{\"labels\":{\"instance_id\":\"9999999999999" +
            "999999\",\"project_id\":\"sandbox-00\",\"zone\":\"us-east1-b\"},\"type\":\"gce_instance\"},\"" +
            "textPayload\":\"Sep 18 22:15:38 emit-bastion sshd[2644]: Accepted publickey for riker from 12" +
            "7.0.0.1 port 58530 ssh2: RSA SHA256:dd/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"timestamp" +
            "\":\"2018-09-18T22:15:38Z\"}";

        GenericEvent e = new EventProcessor().parse(buf);
        assertNotNull(e);
        assertEquals(buf, e.getPayload());
        assertEquals(Detail.Type.AUTH, e.getDetailType());
        AuthDetail ad = e.getDetail();
        assertEquals("riker", ad.getUser());
        assertEquals("127.0.0.1", ad.getSourceAddress());
        assertEquals("emit-bastion", ad.getObject());
        assertEquals(969315338000L, e.getTimestamp().getMillis());
    }

    public void testProcessSSHBadTimestamp() throws Exception {
        String buf = "{\"insertId\":\"f8p4mz1a3ldcos1xz\",\"labels\":{\"compute.googleapis.com/resource_" +
            "name\":\"emit-bastion\"},\"logName\":\"projects/sandbox-00/logs/syslog\",\"receiveTimestamp\"" +
            ":\"2018-09-20T18:43:38.318580313Z\",\"resource\":{\"labels\":{\"instance_id\":\"9999999999999" +
            "999999\",\"project_id\":\"sandbox-00\",\"zone\":\"us-east1-b\"},\"type\":\"gce_instance\"},\"" +
            "textPayload\":\"ZZZ 18 00:00:00 emit-bastion sshd[2644]: Accepted publickey for riker from 12" +
            "7.0.0.1 port 58530 ssh2: RSA SHA256:dd/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"timestamp" +
            "\":\"2018-09-18T22:15:38Z\"}";

        GenericEvent e = new EventProcessor().parse(buf);
        assertNotNull(e);
        assertEquals(buf, e.getPayload());
        assertEquals(Detail.Type.AUTH, e.getDetailType());
        AuthDetail ad = e.getDetail();
        assertEquals("riker", ad.getUser());
        assertEquals("127.0.0.1", ad.getSourceAddress());
        assertEquals("emit-bastion", ad.getObject());
    }
}
