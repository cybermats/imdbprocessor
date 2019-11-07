package org.cybermats;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ImdbProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ImdbProcessor.class);

    public static void main(String[] args) {
        LOG.info("Starting...");
        ImdbProcessorOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(ImdbProcessorOptions.class);

        Pipeline p = Pipeline.create(options);
        p.apply("Read file", TextIO.read().from(options.getInputFile()))
                .apply("Parse TSV", ParDo.of(new ParseTSVFn(options.getHeaders())))
                .apply(Filter.by(new FilterPredicate(options.getFilterColumn(), options.getFilterValues())))
                .apply("Create entity", ParDo.of(new BuildEntityFn(options.getEntity(),
                        options.getProperties(), options.getPropertyTypes(), options.getIdHeader())))
                .apply("Write to datastore", DatastoreIO.v1().write().withProjectId(options.getDatastoreProject()));

        p.run();
    }

}
