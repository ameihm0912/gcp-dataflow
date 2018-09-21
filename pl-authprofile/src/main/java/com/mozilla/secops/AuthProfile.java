package com.mozilla.secops;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import com.mozilla.secops.GenericEvent;
import com.mozilla.secops.Detail;
import com.mozilla.secops.EventProcessor;
import com.mozilla.secops.AuthDetail;
import com.mozilla.secops.State;
import com.mozilla.secops.MemcachedStateInterface;

import java.io.IOException;
import java.lang.IllegalArgumentException;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

class StateModel {
    private String identity;
    private Map<String,ModelEntry> entries;

    private long pruneAge;

    static class ModelEntry {
        private DateTime timestamp;

        public DateTime getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(DateTime ts) {
            timestamp = ts;
        }

        @JsonCreator
        ModelEntry(@JsonProperty("timestamp") DateTime ts) {
            timestamp = ts;
        }
    }

    public Boolean knownAddress(String address) {
        return entries.get(address) != null;
    }

    public Map<String,ModelEntry> getEntries() {
        return entries;
    }

    public String getIdentity() {
        return identity;
    }

    private void pruneOldEntries() {
        Iterator it = entries.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry p = (Map.Entry)it.next();
            ModelEntry me = (ModelEntry)p.getValue();
            long mts = me.getTimestamp().getMillis() / 1000;
            if ((DateTimeUtils.currentTimeMillis() / 1000) - mts > pruneAge) {
                it.remove();
            }
        }
    }

    public Boolean updateModel(String address, DateTime ts) {
        pruneOldEntries();
        Boolean isNew = false;
        ModelEntry newent = new ModelEntry(ts);
        if (entries.get(address) == null) {
            isNew = true;
        }
        entries.put(address, newent);
        return isNew;
    }

    @JsonCreator
    StateModel(@JsonProperty("identity") String user) {
        identity = user;
        entries = new HashMap<String,ModelEntry>();
        pruneAge = 60 * 60 * 24 * 10; // Default 10 days for prune age
    }
}

class ParseFn extends DoFn<String,KV<String,GenericEvent>> {
    private EventProcessor ep;

    ParseFn() {
        ep = new EventProcessor();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        GenericEvent e = ep.parse(c.element());
        if (e.getDetailType() == GenericDetail.Type.AUTH) {
            AuthDetail ad = e.getDetail();
            c.output(KV.of(ad.getUser(), e));
        }
    }
}

class AnalyzeFn extends DoFn<KV<String,Iterable<GenericEvent>>,String> {
    private final Logger log;
    private State state;
    private Boolean initialized;
    private ValueProvider<String> memcachedHost;

    AnalyzeFn(ValueProvider<String> mch) {
        log = LoggerFactory.getLogger(AnalyzeFn.class);
        memcachedHost = mch;
    }

    @Setup
    public void Setup() throws IOException, IllegalArgumentException {
        log.info("Performing DoFn setup stage");
        if (memcachedHost.isAccessible() && memcachedHost.get() != null) {
            String mch = memcachedHost.get();
            log.info("Initializing memcached state connection to {}", mch);
            state = new State(new MemcachedStateInterface(mch));
        } else {
            throw new IllegalArgumentException("no state mechanism specified");
        }
    }

    @Teardown
    public void Teardown() {
        state.done();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        Iterable<GenericEvent> events = c.element().getValue();
        String u = c.element().getKey();
        for (GenericEvent e : events) {
            AuthDetail ad = e.getDetail();
            String address = ad.getSourceAddress();
            StateModel sm = null;

            Boolean willCreate = false;
            sm = state.get(u, StateModel.class);
            if (sm == null) {
                log.info("State model for {} not found, will create", u);
                willCreate = true;
            }

            if (willCreate) {
                sm = new StateModel(u);
                sm.updateModel(address, new DateTime());
                state.set(u, sm);
                continue;
            }

            Boolean isNew = sm.updateModel(address, new DateTime());
            if (!isNew) {
                continue;
            }
            log.info("New model entry for {}, {}", u, address);
        }
    }
}

public class AuthProfile {
    public interface AuthProfileOptions extends PipelineOptions {
        @Description("Input file for direct runner")
        String getInputFile();
        void setInputFile(String value);

        @Description("Read user specification from resource location")
        String getSpecResourcePath();
        void setSpecResourcePath(String value);

        @Description("Enable memcached state using host")
        ValueProvider<String> getMemcachedHost();
        void setMemcachedHost(ValueProvider<String> value);
    }

    static void runAuthProfile(AuthProfileOptions options) throws Exception {
        final Logger log = LoggerFactory.getLogger(AuthProfile.class);
        UserSpec spec;
        log.info("Initializing pipeline");
        Pipeline p = Pipeline.create();

        if (options.getSpecResourcePath() != null) {
            spec = new UserSpec(UserSpec.class.getResourceAsStream(options.getSpecResourcePath()));
        } else {
            throw new IllegalArgumentException("no user specification provided");
        }

        PCollection<String> input;
        if (options.getInputFile() != null) {
            input = p.apply("ReadInput", TextIO.read().from(options.getInputFile()));
        } else {
            throw new IllegalArgumentException("no valid input specified");
        }

        input.apply(ParDo.of(new ParseFn()))
            .apply(GroupByKey.<String, GenericEvent>create())
            .apply(ParDo.of(new AnalyzeFn(options.getMemcachedHost())));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) throws Exception {
        AuthProfileOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(AuthProfileOptions.class);
        runAuthProfile(options);
    }
}
