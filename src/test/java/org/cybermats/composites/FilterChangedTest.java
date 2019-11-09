package org.cybermats.composites;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.cybermats.helpers.EntityHelper;
import org.junit.Rule;
import org.junit.Test;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;

public class FilterChangedTest {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    private Entity createEntity(String projectId, String kind, String id, String property, String value) {
        Key.Builder keyBuilder = makeKey(kind, id);
        PartitionId.Builder pBuilder = keyBuilder.getPartitionIdBuilder();
        pBuilder.setProjectId(projectId);
        keyBuilder.setPartitionId(pBuilder.build());
        Key key = keyBuilder.build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties(property, EntityHelper.createValue(value));
        return entityBuilder.build();
    }

    @Test
    public void TestPositive() {
        Entity oldEnt = createEntity("pid", "kind", "id", "prop", "value");
        Entity newEnt = createEntity("pid", "kind", "id", "prop", "value");
        PCollection<Entity> oldEnts = testPipeline.apply("Phony old searches", Create.of(oldEnt));
        PCollection<Entity> newEnts = testPipeline.apply("Phony new searches", Create.of(newEnt));

        FilterChanged fc = new FilterChanged();
        PCollection<Entity> result = PCollectionTuple.of(fc.getOldSearchTag(), oldEnts)
                .and(fc.getNewSearchTag(), newEnts)
                .apply("Find updated searches", fc);

        PAssert.that(result).empty();
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void TestUpdated() {
        Entity oldEnt = createEntity("pid", "kind", "id", "prop", "value");
        Entity newEnt = createEntity("pid", "kind", "id", "prop", "value2");
        PCollection<Entity> oldEnts = testPipeline.apply("Phony old searches", Create.of(oldEnt));
        PCollection<Entity> newEnts = testPipeline.apply("Phony new searches", Create.of(newEnt));

        FilterChanged fc = new FilterChanged();
        PCollection<Entity> result = PCollectionTuple.of(fc.getOldSearchTag(), oldEnts)
                .and(fc.getNewSearchTag(), newEnts)
                .apply("Find updated searches", fc);

        PAssert.that(result).containsInAnyOrder(newEnt);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void TestNew() {
        Entity newEnt = createEntity("pid", "kind", "id", "prop", "value");
        PCollection<Entity> oldEnts = testPipeline.apply("Phony old searches", Create.empty(TypeDescriptor.of(Entity.class)));
        PCollection<Entity> newEnts = testPipeline.apply("Phony new searches", Create.of(newEnt));

        FilterChanged fc = new FilterChanged();
        PCollection<Entity> result = PCollectionTuple.of(fc.getOldSearchTag(), oldEnts)
                .and(fc.getNewSearchTag(), newEnts)
                .apply("Find updated searches", fc);

        PAssert.that(result).containsInAnyOrder(newEnt);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void TestOld() {
        Entity oldEnt = createEntity("pid", "kind", "id", "prop", "value");
        PCollection<Entity> oldEnts = testPipeline.apply("Phony old searches", Create.of(oldEnt));
        PCollection<Entity> newEnts = testPipeline.apply("Phony new searches", Create.empty(TypeDescriptor.of(Entity.class)));

        FilterChanged fc = new FilterChanged();
        PCollection<Entity> result = PCollectionTuple.of(fc.getOldSearchTag(), oldEnts)
                .and(fc.getNewSearchTag(), newEnts)
                .apply("Find updated searches", fc);

        PAssert.that(result).empty();
        testPipeline.run().waitUntilFinish();
    }

}