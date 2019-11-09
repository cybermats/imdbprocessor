package org.cybermats.transforms;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.cybermats.info.BasicInfo;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SearchGeneratorFnTest {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    private void harness(String id, String title, String[] terms) {
        BasicInfo b = BasicInfo.of(id).setPrimaryTitle(title);
        PCollection<BasicInfo> input = testPipeline.apply(Create.of(b));
        SearchGeneratorFn fn = new SearchGeneratorFn();

        PCollection<KV<String, String>> output = input.apply(ParDo.of(fn));


        List<KV<String, String>> expected = new ArrayList<>();
        for (String term : terms) {
            expected.add(KV.of(term, id));
        }

        PAssert.that(output).containsInAnyOrder(expected);
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void TestSingleWord() {
        String id = "123";
        String title = "abc";
        String[] terms = new String[]{"abc"};
        harness(id, title, terms);
    }

    @Test
    public void TestTwoWords() {
        String id = "123";
        String title = "abc def";
        String[] terms = new String[]{"abc", "def"};
        harness(id, title, terms);
    }

    @Test
    public void TestUpperCaseWords() {
        String id = "123";
        String title = "Abc def";
        String[] terms = new String[]{"abc", "def"};
        harness(id, title, terms);
    }

    @Test
    public void TestCommasWords() {
        String id = "123";
        String title = "abc, def";
        String[] terms = new String[]{"abc", "def"};
        harness(id, title, terms);
    }

    @Test
    public void TestQuotesWords() {
        String id = "123";
        String title = "\"abc def\"";
        String[] terms = new String[]{"abc", "def"};
        harness(id, title, terms);
    }

    @Test
    public void TestDashWords() {
        String id = "123";
        String title = "abc-def";
        String[] terms = new String[]{"abc", "def"};
        harness(id, title, terms);
    }

    @Test
    public void TestSpacesWords() {
        String id = "123";
        String title = "abc  def";
        String[] terms = new String[]{"abc", "def"};
        harness(id, title, terms);
    }

    @Test
    public void TestOnlyCharactersWords() {
        String id = "123";
        String title = "abc \"¤%  def";
        String[] terms = new String[]{"abc", "def"};
        harness(id, title, terms);
    }

    @Test
    public void TestHashTagsWords() {
        String id = "123";
        String title = "abc #def";
        String[] terms = new String[]{"abc", "def"};
        harness(id, title, terms);
    }


}