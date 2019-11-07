package org.cybermats;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.NullValue;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

public class TestBuildEntityFn {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    private final transient ValueProvider<String> kind = ValueProvider.StaticValueProvider.of("kind");
    private final transient ValueProvider<String> idHeader = ValueProvider.StaticValueProvider.of("id");


    @Test
    public void testSimpleBuilding() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("id", "abc");
        map.put("bar", "def");
        inputData.add(map);

        final ValueProvider<String[]> properties = ValueProvider.StaticValueProvider.of(new String[]{
                "bar"
        });
        final ValueProvider<String[]> propertyTypes = ValueProvider.StaticValueProvider.of(new String[]{
                "string"
        });

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        BuildEntityFn fn = new BuildEntityFn(kind, properties, propertyTypes, idHeader);
        PCollection<Entity> output = input.apply(ParDo.of(fn));

        final List<Entity> expected = new ArrayList<>();
        Key key = makeKey(kind.get(), "abc").build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("bar", makeValue("def").setExcludeFromIndexes(true).build());
        expected.add(entityBuilder.build());

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testSimpleBuildingOfTwoExcludingOne() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("id", "abc");
        map.put("foo", "def");
        map.put("bar", "ghi");
        inputData.add(map);

        final ValueProvider<String[]> properties = ValueProvider.StaticValueProvider.of(new String[]{
                "foo"
        });
        final ValueProvider<String[]> propertyTypes = ValueProvider.StaticValueProvider.of(new String[]{
                "string"
        });

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        BuildEntityFn fn = new BuildEntityFn(kind, properties, propertyTypes, idHeader);
        PCollection<Entity> output = input.apply(ParDo.of(fn));

        final List<Entity> expected = new ArrayList<>();
        Key key = makeKey(kind.get(), "abc").build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("foo", makeValue("def").setExcludeFromIndexes(true).build());
        expected.add(entityBuilder.build());

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testSimpleBuildingOfTwoIncludingBoth() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("id", "abc");
        map.put("foo", "def");
        map.put("bar", "ghi");
        inputData.add(map);

        final ValueProvider<String[]> properties = ValueProvider.StaticValueProvider.of(new String[]{
                "foo",
                "bar"
        });
        final ValueProvider<String[]> propertyTypes = ValueProvider.StaticValueProvider.of(new String[]{
                "string",
                "string"
        });

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        BuildEntityFn fn = new BuildEntityFn(kind, properties, propertyTypes, idHeader);
        PCollection<Entity> output = input.apply(ParDo.of(fn));

        final List<Entity> expected = new ArrayList<>();
        Key key = makeKey(kind.get(), "abc").build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("foo", makeValue("def").setExcludeFromIndexes(true).build());
        entityBuilder.putProperties("bar", makeValue("ghi").setExcludeFromIndexes(true).build());
        expected.add(entityBuilder.build());

        Entity entA = entityBuilder.build();
        Entity entE = entityBuilder.build();
        Assert.assertEquals(entA, entE);

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testNullValue() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("id", "abc");
        map.put("foo", "def");
        map.put("bar", null);
        inputData.add(map);

        final ValueProvider<String[]> properties = ValueProvider.StaticValueProvider.of(new String[]{
                "foo",
                "bar"
        });
        final ValueProvider<String[]> propertyTypes = ValueProvider.StaticValueProvider.of(new String[]{
                "string",
                "string"
        });

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        BuildEntityFn fn = new BuildEntityFn(kind, properties, propertyTypes, idHeader);
        PCollection<Entity> output = input.apply(ParDo.of(fn));

        final List<Entity> expected = new ArrayList<>();
        Key key = makeKey(kind.get(), "abc").build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("foo", makeValue("def").setExcludeFromIndexes(true).build());
        entityBuilder.putProperties("bar", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
        expected.add(entityBuilder.build());

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testSimpleIntBuilding() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("id", "abc");
        map.put("bar", "1");
        inputData.add(map);

        final ValueProvider<String[]> properties = ValueProvider.StaticValueProvider.of(new String[]{
                "bar"
        });
        final ValueProvider<String[]> propertyTypes = ValueProvider.StaticValueProvider.of(new String[]{
                "int"
        });

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        BuildEntityFn fn = new BuildEntityFn(kind, properties, propertyTypes, idHeader);
        PCollection<Entity> output = input.apply(ParDo.of(fn));

        final List<Entity> expected = new ArrayList<>();
        Key key = makeKey(kind.get(), "abc").build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("bar", makeValue(1).setExcludeFromIndexes(true).build());
        expected.add(entityBuilder.build());

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testSimpleFloatBuilding() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("id", "abc");
        map.put("bar", "1.0");
        inputData.add(map);

        final ValueProvider<String[]> properties = ValueProvider.StaticValueProvider.of(new String[]{
                "bar"
        });
        final ValueProvider<String[]> propertyTypes = ValueProvider.StaticValueProvider.of(new String[]{
                "float"
        });

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        BuildEntityFn fn = new BuildEntityFn(kind, properties, propertyTypes, idHeader);
        PCollection<Entity> output = input.apply(ParDo.of(fn));

        final List<Entity> expected = new ArrayList<>();
        Key key = makeKey(kind.get(), "abc").build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("bar", makeValue(1.0).setExcludeFromIndexes(true).build());
        expected.add(entityBuilder.build());

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testSimpleFloat2Building() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("id", "abc");
        map.put("bar", "1");
        inputData.add(map);

        final ValueProvider<String[]> properties = ValueProvider.StaticValueProvider.of(new String[]{
                "bar"
        });
        final ValueProvider<String[]> propertyTypes = ValueProvider.StaticValueProvider.of(new String[]{
                "float"
        });

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        BuildEntityFn fn = new BuildEntityFn(kind, properties, propertyTypes, idHeader);
        PCollection<Entity> output = input.apply(ParDo.of(fn));

        final List<Entity> expected = new ArrayList<>();
        Key key = makeKey(kind.get(), "abc").build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("bar", makeValue(1.0).setExcludeFromIndexes(true).build());
        expected.add(entityBuilder.build());

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testComplexBuilding() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("id", "abc");
        map.put("foo", "1");
        map.put("bar", "1");
        map.put("zoo", "1");
        map.put("zaa", "1");
        inputData.add(map);

        final ValueProvider<String[]> properties = ValueProvider.StaticValueProvider.of(new String[]{
                "foo",
                "bar",
                "zoo",
                "zaa"
        });
        final ValueProvider<String[]> propertyTypes = ValueProvider.StaticValueProvider.of(new String[]{
                "string",
                "int",
                "float",
                "boba"
        });

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        BuildEntityFn fn = new BuildEntityFn(kind, properties, propertyTypes, idHeader);
        PCollection<Entity> output = input.apply(ParDo.of(fn));

        final List<Entity> expected = new ArrayList<>();
        Key key = makeKey(kind.get(), "abc").build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("foo", makeValue("1").setExcludeFromIndexes(true).build());
        entityBuilder.putProperties("bar", makeValue(1).setExcludeFromIndexes(true).build());
        entityBuilder.putProperties("zoo", makeValue(1.0).setExcludeFromIndexes(true).build());
        entityBuilder.putProperties("zaa", makeValue("1").setExcludeFromIndexes(true).build());
        expected.add(entityBuilder.build());

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testComplexNullBuilding() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("id", "abc");
        map.put("foo", null);
        map.put("bar", null);
        map.put("zoo", null);
        map.put("zaa", null);
        inputData.add(map);

        final ValueProvider<String[]> properties = ValueProvider.StaticValueProvider.of(new String[]{
                "foo",
                "bar",
                "zoo",
                "zaa"
        });
        final ValueProvider<String[]> propertyTypes = ValueProvider.StaticValueProvider.of(new String[]{
                "string",
                "int",
                "float",
                "boba"
        });

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        BuildEntityFn fn = new BuildEntityFn(kind, properties, propertyTypes, idHeader);
        PCollection<Entity> output = input.apply(ParDo.of(fn));

        final List<Entity> expected = new ArrayList<>();
        Key key = makeKey(kind.get(), "abc").build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("foo", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
        entityBuilder.putProperties("bar", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
        entityBuilder.putProperties("zoo", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
        entityBuilder.putProperties("zaa", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
        expected.add(entityBuilder.build());

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }
}
