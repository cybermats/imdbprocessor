package org.cybermats.imdbprocessor;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

@SuppressWarnings("unused")
public interface ImdbProcessorOptions extends PipelineOptions {
    @Description("Directory/Bucket of the input files.")
    @Validation.Required
    ValueProvider<String> getInputDir();

    void setInputDir(ValueProvider<String> value);

    @Description("Name of show entity type in Datastore.")
    @Validation.Required
    ValueProvider<String> getShowEntity();

    void setShowEntity(ValueProvider<String> value);

    @Description("Name of episode entity type in Datastore.")
    @Validation.Required
    ValueProvider<String> getEpisodeEntity();

    void setEpisodeEntity(ValueProvider<String> value);

    @Description("Name of search entity type in Datastore.")
    @Validation.Required
    ValueProvider<String> getSearchEntity();

    void setSearchEntity(ValueProvider<String> value);

    @Description("Project ID to read from datastore")
    @Validation.Required
    ValueProvider<String> getDatastoreProject();

    void setDatastoreProject(ValueProvider<String> value);
}
