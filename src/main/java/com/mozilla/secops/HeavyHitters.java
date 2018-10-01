package com.mozilla.secops;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

import com.mozilla.secops.parser.Event;
import com.mozilla.secops.parser.Parser;
import com.mozilla.secops.parser.Payload;
import com.mozilla.secops.parser.GLB;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.Duration;
import java.time.ZoneId;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HeavyHitters implements Serializable {
    private class ParseFn extends DoFn<String,String> {
        private Parser ep;

        ParseFn() {
        }

        @Setup
        public void Setup() {
            ep = new Parser();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Event e = ep.parse(c.element());
            if (e.getPayloadType() == Payload.PayloadType.GLB) {
                GLB g = e.getPayload();
                c.outputWithTimestamp(g.getSourceAddress(), new Instant());
            }
        }
    }

    static class AnalyzeFn extends PTransform<PCollection<KV<String, Long>>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<KV<String, Long>> col) {
            PCollection<Long> counts = col.apply("Extract counts", ParDo.of(
                        new DoFn<KV<String, Long>, Long>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                c.output(c.element().getValue());
                            }
                        }
                        ));
            final PCollectionView<Double> meanVal = counts.apply("Mean of counts",
                    Mean.<Long>globally().asSingletonView());
            PCollection<KV<String, Long>> ret = col.apply(
                    "Analyze",
                    ParDo.of(
                        new DoFn<KV<String, Long>, KV<String,Long>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Double mvThresh = 2.0; // XXX testing set
                                Double mv = c.sideInput(meanVal);
                                if (c.element().getValue() > (mv * mvThresh)) {
                                    c.output(c.element());
                                }
                            }
                        }
                        ).withSideInputs(meanVal));
            return ret;
        }
    }

    static class FileTransformFn extends DoFn<KV<String,Long>,String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().toString());
        }
    }

    static class RowTransformFn extends DoFn<KV<String,Long>,TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow r = new TableRow();
            r.set("srcip", c.element().getKey());
            r.set("count", c.element().getValue());
            DateTime d = new DateTime();
            DateTimeFormatter f = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss");
            r.set("timestamp", f.print(d));
            c.output(r);
        }
    }

    public interface HeavyHittersOptions extends PipelineOptions {
        @Description("Path to file to read input from")
        String getInputFile();
        void setInputFile(String value);

        @Description("Pubsub topic to read input from")
        String getInputPubsub();
        void setInputPubsub(String value);

        @Description("Path to file to write output to")
        String getOutputFile();
        void setOutputFile(String value);

        @Description("BigQuery output specification")
        String getOutputBigQuery();
        void setOutputBigQuery(String value);

        @Description("Threshold value to use for calculation")
        @Default.Double(75.0)
        ValueProvider<Double> getThresholdValue();
        void setThresholdValue(ValueProvider<Double> value);
    }

    static void runHeavyHitters(HeavyHittersOptions options) {
        Pipeline p = Pipeline.create(options);

        PCollection<String> input;
        if (options.getInputFile() != null) {
            input = p.apply("Read input", TextIO.read().from(options.getInputFile()));
        } else if (options.getInputPubsub() != null) {
            input = p.apply("Read input", PubsubIO.readStrings()
                    .fromTopic(options.getInputPubsub()));
        } else {
            throw new IllegalArgumentException("no input specified");
        }

        PCollection<KV<String,Long>> data = input.apply(ParDo.of(new HeavyHitters().new ParseFn()))
            .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(2))))
            .apply(Count.<String>perElement())
            .apply(new AnalyzeFn());

        if (options.getOutputFile() != null) {
            data.apply("Text conversion", ParDo.of(new FileTransformFn()))
                .apply("Output", TextIO.write().to(options.getOutputFile()));
        } else if (options.getOutputBigQuery() != null) {
            data.apply("Row conversion", ParDo.of(new RowTransformFn()))
                .apply("BigQuery output", BigQueryIO.writeTableRows()
                        .to(options.getOutputBigQuery())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        } else {
            throw new IllegalArgumentException("no output specified");
        }

        p.run();
    }

    public static void main(String[] args) {
        HeavyHittersOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(HeavyHittersOptions.class);
        runHeavyHitters(options);
    }
}
