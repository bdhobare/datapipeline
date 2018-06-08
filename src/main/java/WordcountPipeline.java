import com.google.api.services.bigquery.model.TableRow;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Creates a streaming dataflow pipeline that saves events to AVRO and bigquery.
 *
 * TODO: add support for Parquet files and S3
 */
public class WordcountPipeline {

    /** Provides an interface for setting the GCS temp location and streaming mode */
    interface Options extends PipelineOptions {
        String getTempLocation();
        void setTempLocation(String value);

        boolean isStreaming();
        void setStreaming(boolean value);
    }

    public static Dotenv getEnv(){
        return Dotenv.configure()
                .directory("/home/bdhobare/IdeaProjects/mlpipeline/dataflow")
                .ignoreIfMalformed()
                .ignoreIfMissing()
                .load();
    }

    public static void main(String[] args){

        String TOPIC  = getEnv().get("TOPIC");

        // set up pipeline options
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(false);
        Pipeline pipeline = Pipeline.create(options);

        // read message events from PubSub
        PCollection<PubsubMessage> events = pipeline
                .apply(PubsubIO.readMessages().fromTopic(TOPIC));
        // parse the PubSub events and create rows to insert into BigQuery
        events.apply("ReadLines", new ReadLines())
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsRowFn()))
                .apply("Remove Null", Filter.by(p -> p != null))
                .apply("To BigQuery",BigQueryIO.writeTableRows()
                .to(Schema.getTable())
                .withSchema(Schema.getSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));



    }
    /**
     * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
     * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it
     * to a ParDo in the pipeline.
     */
    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        String TOKENIZER_PATTERN = "[^\\p{L}]+";
        private final Distribution lineLenDist = Metrics.distribution(
                ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(ProcessContext c) {
            lineLenDist.update(c.element().length());
            if (c.element().trim().isEmpty()) {
                emptyLines.inc();
            }

            // Split the line into words.
            String[] words = c.element().split(TOKENIZER_PATTERN);

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    c.output(word);
                }
            }
        }
    }

    /** A SimpleFunction that converts a Word and Count into a table row. */
    public static class FormatAsRowFn extends SimpleFunction<KV<String, Long>, TableRow> {
        @Override
        public TableRow apply(KV<String, Long> input) {

            TableRow record = new TableRow();
            record.set("Word", input.getKey());
            record.set("Count", input.getValue());

            return record;
        }
    }


    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     *
     * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
     * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
     * modular testing, and an improved monitoring experience.
     */
    public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(
                    ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

            return wordCounts;
        }
    }
    private static class ReadLines extends PTransform<PCollection<PubsubMessage>, PCollection<String>>{

        public PCollection<String> expand(PCollection<PubsubMessage> input) {
            return input.apply("Read Lines", ParDo.of(new DoFn<PubsubMessage, String>(){
                @ProcessElement
                public void processElement(ProcessContext c) {
                    String line = c.element().getPayload().toString();
                    c.output(line);
                }
            }));
        }
    }
}
