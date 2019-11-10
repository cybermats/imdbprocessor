package org.cybermats.composites;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.cybermats.info.BasicInfo;
import org.cybermats.info.RatingInfo;
import org.cybermats.transforms.ParseBasicInfoFn;
import org.cybermats.transforms.ParseRatingsInfoFn;

public class CreateBasicsWithRatings extends PTransform<PCollectionTuple, PCollection<BasicInfo>> {

    private final TupleTag<String> basicsFileTag = new TupleTag<String>() {
    };
    private final TupleTag<String> ratingsFileTag = new TupleTag<String>() {
    };
    private final TupleTag<RatingInfo> ratingInfoTag = new TupleTag<RatingInfo>() {
    };
    private final TupleTag<BasicInfo> basicInfoTag = new TupleTag<BasicInfo>() {
    };

    public TupleTag<String> getBasicsFileTag() {
        return basicsFileTag;
    }

    public TupleTag<String> getRatingsFileTag() {
        return ratingsFileTag;
    }

    @Override
    public PCollection<BasicInfo> expand(PCollectionTuple inputs) {
        PCollection<String> basicLines = inputs.get(basicsFileTag);
        PCollection<String> ratingLines = inputs.get(ratingsFileTag);


        PCollection<KV<String, BasicInfo>> basicsById = basicLines
                .apply("Parse TSV into map of BasicInfo", ParDo.of(new ParseBasicInfoFn()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(BasicInfo.class)));

        PCollection<KV<String, RatingInfo>> ratingsById = ratingLines
                .apply("Parse TSV into map of RatingInfo", ParDo.of(new ParseRatingsInfoFn()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(RatingInfo.class)));

        return KeyedPCollectionTuple.of(basicInfoTag, basicsById).and(ratingInfoTag, ratingsById)
                .apply("Join basics and ratings", CoGroupByKey.create())
                .apply("Add ratings to basic info", ParDo.of(new DoFn<KV<String, CoGbkResult>, BasicInfo>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();
                        RatingInfo ratings = e.getValue().getOnly(ratingInfoTag, null);
                        if (ratings != null) {
                            Float rating = ratings.getRating();
                            for (BasicInfo b : e.getValue().getAll(basicInfoTag)) {
                                BasicInfo nb = b.getBuilder().setRating(rating).build();
                                c.output(nb);
                            }
                        }
                    }
                }));
    }

}
