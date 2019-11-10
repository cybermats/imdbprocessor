package org.cybermats.composites;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.cybermats.info.BasicInfo;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

public class CreateBasicsWithRatingsTest {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void TestSingleElement() {
        String[] basicInputLines = new String[]{
                "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenre",
                "id\t\\N\ttitle\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"
        };
        String[] ratingInputLines = new String[]{
                "tconst\taverageRating\tnumVotes",
                "id\t1.23\t1"
        };
        PCollection<String> basicInputs = testPipeline
                .apply("Phony basic input", Create.of(Arrays.asList(basicInputLines)));
        PCollection<String> ratingInputs = testPipeline
                .apply("Phony rating input", Create.of(Arrays.asList(ratingInputLines)));

        CreateBasicsWithRatings createBasicsWithRatings = new CreateBasicsWithRatings();

        PCollection<BasicInfo> actual = PCollectionTuple
                .of(createBasicsWithRatings.getBasicsFileTag(), basicInputs)
                .and(createBasicsWithRatings.getRatingsFileTag(), ratingInputs)
                .apply("Create basics with Ratings", createBasicsWithRatings);

        BasicInfo expected = new BasicInfo.Builder("id").setPrimaryTitle("title").setRating(1.23f).build();

        PAssert.that(actual).containsInAnyOrder(expected);

        testPipeline.run().waitUntilFinish();
    }
}