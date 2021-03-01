
package com.beamtest;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessLog {

    private static String ipExpr = "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}";

    private static String[] patterns = {
            "(?<sourceIp>" + ipExpr + ")",
            "(?<destinationIp>" + ipExpr + ")",
            "(?<timestamp>\\d{4}-\\d{1,2}-\\d{1,2}T\\d{2}:\\d{2}:\\d{2}Z)|(?<timestamp2>\\d+)",
            "(?<bytes>\\d+)",
            "(?<sourcePort>\\d+)",
            "(?<destinationPort>\\d+)",
            "(?<authorized>(true|false))",
            "(?<logId>\\d)"
    };

    private static String[] labels = {
            "sourceIp",
            "destinationIp",
            "timestamp",
            "bytes",
            "sourcePort",
            "destinationPort",
            "authorized",
            "logId"
    };

    private static Pattern[] logPatterns = null;

    private static void initializePatterns() {
        logPatterns = new Pattern[patterns.length];
        for (int i=0;i<patterns.length;i++){
            logPatterns[i] = Pattern.compile(patterns[i]);
        }
    }

    static class ExtractItemsFn extends DoFn<String, String[]> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<String[]> receiver) {
            String[] items = line.split(" ");
            receiver.output(items);
        }
    }

    public static class GetLogItems extends PTransform<PCollection<String>, PCollection<String[]>> {
        @Override
        public PCollection<String[]> expand(PCollection<String> lines) {
            PCollection<String[]> items = lines.apply(ParDo.of(new ExtractItemsFn()));
            return items;
        }
    }

    public static class FormatAsJson extends SimpleFunction<String[], String> {
        @Override
        public String apply(String[] items) {
            String[] sb = new String[2];
            sb[0] = null;
            sb[1] = null;
            String prefixed = null;
            StringBuilder onesb = new StringBuilder();
            try {
                for (int i = 0; i < items.length; i++) {
                    Matcher logs = logPatterns[i].matcher(items[i]);
                    logs.find();
                    if (i == 0) onesb.append("{");
                    if (i == 2) {
                        String s1 = logs.group("timestamp");
                        String s2 = logs.group("timestamp2");
                        if (s1 == null) {
                            long t = new Long(s2);
                            Instant instant = Instant.ofEpochMilli(t);
                            String ts = instant.toString();
                            onesb.append("\"" + labels[i] + ":" + "\"" + ts + "\"");
                        } else
                            onesb.append("\"" + labels[i] + ":" + "\"" + s1 + "\"");
                    } else
                        onesb.append("\"" + labels[i] + ":" + "\"" + logs.group(labels[i]) + "\"");
                    if (i == items.length - 1) {
                        onesb.append("}");
                        prefixed = logs.group(labels[i])+onesb.toString();
                    } else
                        onesb.append(",");
                }
            }
            catch (Exception ex) {
                System.out.println(ex.getMessage());
                return "";
            }
            return prefixed;
        }
    }

    public static class removePrefix extends SimpleFunction<String, String> {
        @Override
        public String apply(String line) {
            return line.substring(1);
        }
    }

    public interface ProcessLogOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("input/input.txt")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Required
        String getOutputOne();
        void setOutputOne(String value);

        @Description("Path of the file to write to")
        @Required
        String getOutputTwo();
        void setOutputTwo(String value);

        @Description("Path of the file to write to")
        @Required
        String getOutputArchive();
        void setOutputArchive(String value);

    }

    static void runProcessLogs(ProcessLogOptions options) {
        initializePatterns();
        Pipeline p = Pipeline.create(options);
        PCollection<String> archive = p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(new GetLogItems())
                .apply(MapElements.via(new FormatAsJson()));
        archive.apply(Filter.by(x->!x.equals(""))).apply(MapElements.via(new removePrefix())).apply(TextIO.write().to(options.getOutputArchive()).withoutSharding());
        archive.apply(Filter.by(x->x.startsWith("1"))).apply(MapElements.via(new removePrefix())).apply(TextIO.write().to(options.getOutputOne()).withoutSharding());
        archive.apply(Filter.by(x->x.startsWith("2"))).apply(MapElements.via(new removePrefix())).apply(TextIO.write().to(options.getOutputTwo()).withoutSharding());
     p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        ProcessLogOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ProcessLogOptions.class);
        runProcessLogs(options);
    }
}

