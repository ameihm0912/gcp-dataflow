package com.mozilla.secops;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
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

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.Duration;
import java.time.ZoneId;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HeavyHitters {
    static class ParseFn extends DoFn<String, String> {
        private static final String expression = "^(\\S+) - - \\[(\\S+ \\S+?)\\] .*";
        private static final String dateformat = "dd/MMM/yyyy:HH:mm:ss Z";

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            Pattern pat = Pattern.compile(this.expression);
            Matcher mat = pat.matcher(element);
            if (!mat.matches()) {
                return;
            }
            DateTimeFormatter dfmt = DateTimeFormat.forPattern(this.dateformat);
            Instant itime = dfmt.parseDateTime(mat.group(2)).toInstant();
            receiver.outputWithTimestamp(mat.group(1), itime);
        }
    }

    static class OutputFmt extends DoFn<KV<String, Long>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().toString());
        }
    }

    static class AnalyzeFn extends PTransform<PCollection<KV<String, Long>>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<KV<String, Long>> col) {
            PCollection<Long> counts = col.apply(ParDo.of(
                        new DoFn<KV<String, Long>, Long>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                c.output(c.element().getValue());
                            }
                        }
                        ));
            final PCollectionView<Double> meanVal = counts.apply(Mean.<Long>globally().asSingletonView());
            PCollection<KV<String, Long>> ret = col.apply(
                    "Analyze",
                    ParDo.of(
                        new DoFn<KV<String, Long>, KV<String,Long>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Double mvThresh = 2.0;
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

    public interface HeavyHittersOptions extends PipelineOptions {
        @Description("Path to input log data")
        @Required
        String getInputFile();
        void setInputFile(String value);

        @Description("Path for output")
        @Required
        String getOutputFile();
        void setOutputFile(String value);

        @Description("Threshold value")
        @Default.Double(1.0)
        ValueProvider<Double> getThresholdValue();
        void setThresholdValue(ValueProvider<Double> value);
    }

    static void runHeavyHitters(HeavyHittersOptions options) {
        Pipeline p = Pipeline.create();

        PCollection<KV<String, Long>> col = p.apply("ReadInput", TextIO.read().from(options.getInputFile()))
            .apply(ParDo.of(new ParseFn()))
            .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(2))))
            .apply(Count.<String>perElement());

        col.apply(new AnalyzeFn())
            .apply(ParDo.of(new OutputFmt()))
            .apply("Output", TextIO.write().to(options.getOutputFile()));
            

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        HeavyHittersOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(HeavyHittersOptions.class);
        runHeavyHitters(options);
    }
}
