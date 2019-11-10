package org.cybermats.imdbprocessor.composites;

import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class ReadOldEntities extends PTransform<PBegin, PCollection<Entity>> {
    private final ValueProvider<String> kind;
    private final ValueProvider<String> projectId;


    private ReadOldEntities(ValueProvider<String> kind, ValueProvider<String> projectId) {
        this.kind = kind;
        this.projectId = projectId;
    }

    public static ReadOldEntities of(ValueProvider<String> kind, ValueProvider<String> projectId) {
        return new ReadOldEntities(kind, projectId);
    }

    private static ValueProvider<String> createGqlQuery(ValueProvider<String> kind) {
        return ValueProvider.NestedValueProvider.of(
                kind, (SerializableFunction<String, String>) input -> String.format("SELECT * FROM %s", input));
    }

    @Override
    public PCollection<Entity> expand(PBegin input) {
        return input.apply("Read From Datastore",
                DatastoreIO.v1().read()
                        .withProjectId(projectId)
                        .withLiteralGqlQuery(createGqlQuery(kind)));
    }
}
