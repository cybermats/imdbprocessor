import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

public class ImdbProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ImdbProcessor.class);

    public static void main(String[] args) {
        LOG.info("Starting...");
        PipelineOptions pOptions =
                PipelineOptionsFactory.fromArgs(args).withValidation().create();

        ImdbProcessorOptions options = pOptions.as(ImdbProcessorOptions.class);

        String projectID = "matsf-cloud-gotesting";
        Pipeline p = Pipeline.create(options);
        p.apply("Read file", TextIO.read().from(options.getInputFile()))
                .apply("Parse TSV", ParDo.of(new ParseTSVFn(options.getHeaders())))
                .apply("Create entity", ParDo.of(new BuildEntityFn(options.getEntity(), options.getProperties(), options.getIdHeader())))
                .apply("Write to datastore", DatastoreIO.v1().write().withProjectId(projectID));
    }

    public interface ImdbProcessorOptions extends PipelineOptions {
        @Description("Path of the input file.")
        @Validation.Required
        ValueProvider<String> getInputFile();

        void setInputFile(ValueProvider<String> value);

        @Description("All headers in the file.")
        @Validation.Required
        ValueProvider<String[]> getHeaders();

        void setHeaders(ValueProvider<String[]> value);

        @Description("Name of the header that contains the Id column.")
        @Validation.Required
        ValueProvider<String> getIdHeader();

        void setIdHeader(ValueProvider<String> value);

        @Description("List of headers that should be added as properties.")
        @Validation.Required
        ValueProvider<String[]> getProperties();

        void setProperties(ValueProvider<String[]> value);

        @Description("Name of entity type in Datastore.")
        @Validation.Required
        ValueProvider<String> getEntity();

        void setEntity(ValueProvider<String> value);
/*
        @Description("Project ID to read from datastore")
        @Validation.Required
        ValueProvider<String> getProject();
        void setProject(ValueProvider<String> value);*/
    }

    static class ParseTSVFn extends DoFn<String, HashMap<String, String>> {
        private static final Logger LOG = LoggerFactory.getLogger(ParseTSVFn.class);
        private final ValueProvider<String[]> headers;

        ParseTSVFn(ValueProvider<String[]> headers) {
            this.headers = headers;
        }

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<HashMap<String, String>> receiver) {
            String[] words = element.split("\t");
            if (words.length != headers.get().length) {
                LOG.error("Bad format on input. Actual columns: {}, expected columns: {}.", words.length, headers.get().length);
                return;
            }
            HashMap<String, String> map = new HashMap<String, String>();
            for (int i = 0; i < words.length; i++) {
                map.put(headers.get()[i], words[i]);
            }
            receiver.output(map);
        }
    }

    static class BuildEntityFn extends DoFn<HashMap<String, String>, Entity> {
        private final ValueProvider<String> kind;
        private final ValueProvider<String[]> properties;
        private final ValueProvider<String> idHeader;

        BuildEntityFn(ValueProvider<String> kind, ValueProvider<String[]> properties, ValueProvider<String> idHeader) {
            this.kind = kind;
            this.properties = properties;
            this.idHeader = idHeader;
        }

        public Entity makeEntity(HashMap<String, String> element) {
            final String id = element.get(this.idHeader.get());
            Key key = makeKey(this.kind.get(), id).build();

            Entity.Builder entityBuilder = Entity.newBuilder();
            entityBuilder.setKey(key);

            for (String property : this.properties.get()) {
                Value value = makeValue(element.get(property)).setExcludeFromIndexes(true).build();
                entityBuilder.putProperties(property, value);
            }
            return entityBuilder.build();
        }

        @ProcessElement
        public void processElement(@Element HashMap<String, String> element, OutputReceiver<Entity> receiver) {
            receiver.output(makeEntity(element));
        }
    }
}
