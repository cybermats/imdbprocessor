package org.cybermats;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

interface ImdbProcessorOptions extends PipelineOptions {
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

    @Description("List of headers that should be added as properties.")
    @Validation.Required
    ValueProvider<String[]> getPropertyTypes();

    void setPropertyTypes(ValueProvider<String[]> value);

    @Description("Name of entity type in Datastore.")
    @Validation.Required
    ValueProvider<String> getEntity();

    void setEntity(ValueProvider<String> value);

    @Description("Project ID to read from datastore")
    @Validation.Required
    ValueProvider<String> getDatastoreProject();

    void setDatastoreProject(ValueProvider<String> value);

    @Description("Column to apply filter to.")
    ValueProvider<String> getFilterColumn();

    void setFilterColumn(ValueProvider<String> value);

    @Description("Which value to filter for.")
    ValueProvider<String[]> getFilterValues();

    void setFilterValues(ValueProvider<String[]> value);

}
