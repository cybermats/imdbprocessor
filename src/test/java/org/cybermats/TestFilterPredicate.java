package org.cybermats;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestFilterPredicate {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();


    @Test
    public void testSimpleFiltering() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("foo", "abc");
        map.put("bar", "def");
        inputData.add(map);
        map = new TSVRow();
        map.put("foo", "123");
        map.put("bar", "456");
        inputData.add(map);

        final ValueProvider<String> filterColumn = ValueProvider.StaticValueProvider.of("foo");
        final ValueProvider<String[]> filterValues = ValueProvider.StaticValueProvider.of(new String[]{"abc"});

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        FilterPredicate pred = new FilterPredicate(filterColumn, filterValues);
        PCollection<TSVRow> output = input.apply(Filter.by(pred));


        final List<TSVRow> expected = new ArrayList<>();
        map = new TSVRow();
        map.put("foo", "abc");
        map.put("bar", "def");
        expected.add(map);

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testSimpleFilteringByCase() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("foo", "abc");
        map.put("bar", "def");
        inputData.add(map);
        map = new TSVRow();
        map.put("foo", "123");
        map.put("bar", "456");
        inputData.add(map);

        final ValueProvider<String> filterColumn = ValueProvider.StaticValueProvider.of("foo");
        final ValueProvider<String[]> filterValues = ValueProvider.StaticValueProvider.of(new String[]{"Abc"});

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        FilterPredicate pred = new FilterPredicate(filterColumn, filterValues);
        PCollection<TSVRow> output = input.apply(Filter.by(pred));

        PAssert.that(output).empty();
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testMultipleFiltering() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("foo", "abc");
        map.put("bar", "def");
        inputData.add(map);
        map = new TSVRow();
        map.put("foo", "123");
        map.put("bar", "456");
        inputData.add(map);
        map = new TSVRow();
        map.put("foo", "hello");
        map.put("bar", "world");
        inputData.add(map);

        final ValueProvider<String> filterColumn = ValueProvider.StaticValueProvider.of("foo");
        final ValueProvider<String[]> filterValues = ValueProvider.StaticValueProvider.of(new String[]{"abc", "hello"});

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        FilterPredicate pred = new FilterPredicate(filterColumn, filterValues);
        PCollection<TSVRow> output = input.apply(Filter.by(pred));


        final List<TSVRow> expected = new ArrayList<>();
        map = new TSVRow();
        map.put("foo", "abc");
        map.put("bar", "def");
        expected.add(map);
        map = new TSVRow();
        map.put("foo", "hello");
        map.put("bar", "world");
        expected.add(map);

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testNullFiltering() {
        final List<TSVRow> inputData = new ArrayList<>();
        TSVRow map = new TSVRow();
        map.put("foo", "abc");
        map.put("bar", "def");
        inputData.add(map);
        map = new TSVRow();
        map.put("foo", "123");
        map.put("bar", "456");
        inputData.add(map);

        final ValueProvider<String> filterColumn = ValueProvider.StaticValueProvider.of(null);
        final ValueProvider<String[]> filterValues = ValueProvider.StaticValueProvider.of(null);

        PCollection<TSVRow> input = testPipeline.apply(Create.of(inputData));
        FilterPredicate pred = new FilterPredicate(filterColumn, filterValues);
        PCollection<TSVRow> output = input.apply(Filter.by(pred));


        final List<TSVRow> expected = new ArrayList<>();
        map = new TSVRow();
        map.put("foo", "abc");
        map.put("bar", "def");
        expected.add(map);
        map = new TSVRow();
        map.put("foo", "123");
        map.put("bar", "456");
        expected.add(map);

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }


}