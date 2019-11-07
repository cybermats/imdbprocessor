package org.cybermats;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class TestParseTSVFn {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void testSingleParsing() {
        final String[] LINES_ARRAY = new String[]{
                "abc\tdef",
                "123\t456"
        };
        final List<String> LINES = Arrays.asList(LINES_ARRAY);

        ValueProvider<String[]> headers = ValueProvider.StaticValueProvider.of(new String[]{
                "foo", "bar"
        });

        List<HashMap<String, String>> expected = new ArrayList<>();
        HashMap<String, String> map = new HashMap<>();
        map.put("foo", "abc");
        map.put("bar", "def");
        expected.add(map);
        map = new HashMap<>();
        map.put("foo", "123");
        map.put("bar", "456");
        expected.add(map);


        PCollection<String> input = testPipeline.apply(Create.of(LINES));
        ParseTSVFn fn = new ParseTSVFn(headers);
        PCollection<HashMap<String, String>> output = input.apply(ParDo.of(fn));

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testHeaderRemovedParsing() {
        final String[] LINES_ARRAY = new String[]{
                "foo\tbar",
                "abc\tdef",
                "123\t456"
        };
        final List<String> LINES = Arrays.asList(LINES_ARRAY);

        ValueProvider<String[]> headers = ValueProvider.StaticValueProvider.of(new String[]{
                "foo", "bar"
        });

        List<HashMap<String, String>> expected = new ArrayList<>();
        HashMap<String, String> map = new HashMap<>();
        map.put("foo", "abc");
        map.put("bar", "def");
        expected.add(map);
        map = new HashMap<>();
        map.put("foo", "123");
        map.put("bar", "456");
        expected.add(map);


        PCollection<String> input = testPipeline.apply(Create.of(LINES));
        ParseTSVFn fn = new ParseTSVFn(headers);
        PCollection<HashMap<String, String>> output = input.apply(ParDo.of(fn));

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testNilFieldsRemoved() {
        final String[] LINES_ARRAY = new String[]{
                "abc\tdef",
                "\\N\t456"
        };
        final List<String> LINES = Arrays.asList(LINES_ARRAY);

        ValueProvider<String[]> headers = ValueProvider.StaticValueProvider.of(new String[]{
                "foo", "bar"
        });

        List<HashMap<String, String>> expected = new ArrayList<>();
        HashMap<String, String> map = new HashMap<>();
        map.put("foo", "abc");
        map.put("bar", "def");
        expected.add(map);
        map = new HashMap<>();
        map.put("foo", null);
        map.put("bar", "456");
        expected.add(map);


        PCollection<String> input = testPipeline.apply(Create.of(LINES));
        ParseTSVFn fn = new ParseTSVFn(headers);
        PCollection<HashMap<String, String>> output = input.apply(ParDo.of(fn));

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }


}