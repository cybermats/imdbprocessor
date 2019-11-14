package org.cybermats.imdbprocessor.composites;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.cybermats.imdbprocessor.helpers.EntityHelper;
import org.junit.Rule;
import org.junit.Test;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;

@SuppressWarnings("SameParameterValue")
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
        PCollection<Entity> oldEntities = testPipeline.apply("Phony old searches", Create.of(oldEnt));
        PCollection<Entity> newEntities = testPipeline.apply("Phony new searches", Create.of(newEnt));

        FilterChanged fc = new FilterChanged();
        PCollectionTuple result = PCollectionTuple.of(fc.getOldSearchTag(), oldEntities)
                .and(fc.getNewSearchTag(), newEntities)
                .apply("Find updated searches", fc);

        PCollection<Entity> deleteResult = result.get(fc.getDeletedEntityTag());
        PCollection<Entity> upsertResult = result.get(fc.getUpsertEntityTag());

        PAssert.that(deleteResult).empty();
        PAssert.that(upsertResult).empty();
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void TestUpdated() {
        Entity oldEnt = createEntity("pid", "kind", "id", "prop", "value");
        Entity newEnt = createEntity("pid", "kind", "id", "prop", "value2");
        PCollection<Entity> oldEntities = testPipeline.apply("Phony old searches", Create.of(oldEnt));
        PCollection<Entity> newEntities = testPipeline.apply("Phony new searches", Create.of(newEnt));

        FilterChanged fc = new FilterChanged();
        PCollectionTuple result = PCollectionTuple.of(fc.getOldSearchTag(), oldEntities)
                .and(fc.getNewSearchTag(), newEntities)
                .apply("Find updated searches", fc);

        PCollection<Entity> deleteResult = result.get(fc.getDeletedEntityTag());
        PCollection<Entity> upsertResult = result.get(fc.getUpsertEntityTag());

        PAssert.that(deleteResult).empty();
        PAssert.that(upsertResult).containsInAnyOrder(newEnt);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void TestNew() {
        Entity newEnt = createEntity("pid", "kind", "id", "prop", "value");
        PCollection<Entity> oldEntities = testPipeline.apply("Phony old searches", Create.empty(TypeDescriptor.of(Entity.class)));
        PCollection<Entity> newEntities = testPipeline.apply("Phony new searches", Create.of(newEnt));

        FilterChanged fc = new FilterChanged();
        PCollectionTuple result = PCollectionTuple.of(fc.getOldSearchTag(), oldEntities)
                .and(fc.getNewSearchTag(), newEntities)
                .apply("Find updated searches", fc);

        PCollection<Entity> deleteResult = result.get(fc.getDeletedEntityTag());
        PCollection<Entity> upsertResult = result.get(fc.getUpsertEntityTag());

        PAssert.that(deleteResult).empty();
        PAssert.that(upsertResult).containsInAnyOrder(newEnt);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void TestOld() {
        Entity oldEnt = createEntity("pid", "kind", "id", "prop", "value");
        PCollection<Entity> oldEntities = testPipeline.apply("Phony old searches", Create.of(oldEnt));
        PCollection<Entity> newEntities = testPipeline.apply("Phony new searches", Create.empty(TypeDescriptor.of(Entity.class)));

        FilterChanged fc = new FilterChanged();
        PCollectionTuple result = PCollectionTuple.of(fc.getOldSearchTag(), oldEntities)
                .and(fc.getNewSearchTag(), newEntities)
                .apply("Find updated searches", fc);

        PCollection<Entity> deleteResult = result.get(fc.getDeletedEntityTag());
        PCollection<Entity> upsertResult = result.get(fc.getUpsertEntityTag());

        PAssert.that(deleteResult).containsInAnyOrder(oldEnt);
        PAssert.that(upsertResult).empty();
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void TestMoved() {
        Entity oldEnt = createEntity("pid", "kind", "id", "prop", "value");
        Entity newEnt = createEntity("pid2", "kind", "id", "prop", "value");
        PCollection<Entity> oldEntities = testPipeline.apply("Phony old searches", Create.of(oldEnt));
        PCollection<Entity> newEntities = testPipeline.apply("Phony new searches", Create.of(newEnt));

        FilterChanged fc = new FilterChanged();
        PCollectionTuple result = PCollectionTuple.of(fc.getOldSearchTag(), oldEntities)
                .and(fc.getNewSearchTag(), newEntities)
                .apply("Find updated searches", fc);

        PCollection<Entity> deleteResult = result.get(fc.getDeletedEntityTag());
        PCollection<Entity> upsertResult = result.get(fc.getUpsertEntityTag());

        PAssert.that(deleteResult).containsInAnyOrder(oldEnt);
        PAssert.that(upsertResult).containsInAnyOrder(newEnt);
        testPipeline.run().waitUntilFinish();
    }


}