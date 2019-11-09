package org.cybermats;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.cybermats.data.EpisodeData;
import org.cybermats.data.SearchData;
import org.cybermats.data.ShowData;
import org.cybermats.info.BasicInfo;
import org.cybermats.info.LinkInfo;
import org.cybermats.info.RatingInfo;
import org.cybermats.transforms.BuildEpisodeDataFn;
import org.cybermats.transforms.BuildSearchDataFn;
import org.cybermats.transforms.BuildShowDataFn;
import org.cybermats.transforms.ParseTSVFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ImdbProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ImdbProcessor.class);

    private static final String[] BASIC_HEADERS = new String[]{"tconst", "titleType", "primaryTitle",
            "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genre"};
    private static final String[] EPISODE_HEADERS = new String[]{"tconst", "parentTconst",
            "seasonNumber", "episodeNumber"};
    private static final String[] RATING_HEADERS = new String[]{"tconst", "averageRating", "numVotes"};

    private static ValueProvider.NestedValueProvider<String, String> createFilename(
            ValueProvider<String> directory, String filename) {
        return ValueProvider.NestedValueProvider.of(
                directory, (SerializableFunction<String, String>) dir -> {
                    String path = dir;
                    if (!path.endsWith("/")) {
                        path += "/";
                    }
                    path += filename;
                    return path;
                }
        );
    }

    public static void main(String[] args) {
        LOG.info("Starting...");
        ImdbProcessorOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(ImdbProcessorOptions.class);

        final TupleTag<RatingInfo> ratingTag = new TupleTag<RatingInfo>() {
        };
        final TupleTag<LinkInfo> linkTag = new TupleTag<LinkInfo>() {
        };
        final TupleTag<BasicInfo> basicTag = new TupleTag<BasicInfo>() {
        };
        final TupleTag<EpisodeData> episodeTag = new TupleTag<EpisodeData>() {
        };


        Pipeline p = Pipeline.create(options);

        /*
         Read the Basic and Ratings file, and populate the basic info with the ratings.
         */
        // TODO: Remove TSV parsing step and join everything into one transform. gs -> map basic on ID.
        PCollection<KV<String, BasicInfo>> basicsById = p.apply("Read Basics File", TextIO.read().from(
                createFilename(options.getInputDir(), "title.basics.tsv.gz")))
                .apply("Parse Basics TSV", ParDo.of(new ParseTSVFn(BASIC_HEADERS)))
                .apply("Convert Basics TSV into POJO", MapElements.into(TypeDescriptor.of(BasicInfo.class)).via(BasicInfo::new))
                .apply("Map Basics over Id", WithKeys.of((SerializableFunction<BasicInfo, String>) BasicInfo::getTConst))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(BasicInfo.class)));

        // TODO: Remove TSV parsing step and join everything into one transform. gs -> map ratings on ID.
        PCollection<KV<String, RatingInfo>> ratingsById = p.apply("Read Ratings File",
                TextIO.read().from(createFilename(options.getInputDir(), "title.ratings.tsv.gz")))
                .apply("Parse Ratings TSV", ParDo.of(new ParseTSVFn(RATING_HEADERS)))
                .apply("Convert Ratings TSV into POJO", MapElements.into(TypeDescriptor.of(RatingInfo.class)).via(RatingInfo::new))
                .apply("Map Ratings over Id", WithKeys.of((SerializableFunction<RatingInfo, String>) RatingInfo::getTConst))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(RatingInfo.class)));

        PCollection<KV<String, CoGbkResult>> basicsWithRatingsPrevious =
                KeyedPCollectionTuple.of(basicTag, basicsById).and(ratingTag, ratingsById)
                        .apply("Join basics and ratings", CoGroupByKey.create());

        PCollection<BasicInfo> basicsWithRatings = basicsWithRatingsPrevious
                .apply("Add ratings to basic info", ParDo.of(new DoFn<KV<String, CoGbkResult>, BasicInfo>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();
                        RatingInfo ratings = e.getValue().getOnly(ratingTag, null);
                        if (ratings != null) {
                            Float rating = ratings.getRating();
                            for (BasicInfo b : e.getValue().getAll(basicTag)) {
                                b.setRating(rating);
                                c.output(b);
                            }
                        }
                    }
                }));


        /*
         Read the episodes file to get the link to the parent for each episode.
         */

        // Filter out only the episode info.
        PCollection<KV<String, BasicInfo>> allEpisodesById = basicsWithRatings
                .apply("Filter tvEpisodes", Filter.by((BasicInfo b) -> b.getTitleType().equals("tvEpisode")))
                .apply("Map Episodes over Id", WithKeys.of(BasicInfo::getTConst))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(BasicInfo.class)));

        // TODO: Remove TSV parsing step and join everything into one transform. gs -> map links on ID.
        // Read Episode link info into map of POJO objects.
        PCollection<KV<String, LinkInfo>> linksById = p.apply("Read Episode File",
                TextIO.read().from(createFilename(options.getInputDir(), "title.episode.tsv.gz")))
                .apply("Parse TSV", ParDo.of(new ParseTSVFn(EPISODE_HEADERS)))
                .apply("Convert Episode TSV into POJO", MapElements.into(TypeDescriptor.of(LinkInfo.class)).via(LinkInfo::new))
                .apply("Map link info over Id", WithKeys.of(LinkInfo::getTConst))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(LinkInfo.class)));

        PCollection<KV<String, EpisodeData>> episodesByParents =
                KeyedPCollectionTuple.of(basicTag, allEpisodesById).and(linkTag, linksById)
                        .apply("Join Episodes with Link info to get Parent", CoGroupByKey.create())
                        .apply("Group Episodes by parent",
                                ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, EpisodeData>>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        KV<String, CoGbkResult> e = c.element();
                                        LinkInfo link = e.getValue().getOnly(linkTag, null);
                                        if (link != null) {
                                            for (BasicInfo b : e.getValue().getAll(basicTag)) {
                                                EpisodeData eData = new EpisodeData.Builder()
                                                        .addBasicInfo(b).addLinkInfo(link).build();
                                                c.output(KV.of(eData.getParentTConst(), eData));
                                            }
                                        }
                                    }
                                }))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(EpisodeData.class)));



        /*
          Create show data where shows are created from basics and episode info.
         */

        // Filter out only the season info.
        PCollection<BasicInfo> allTvSeries = basicsWithRatings.apply("Filter shows",
                Filter.by((BasicInfo b) -> b.getTitleType().equals("tvSeries") ||
                        b.getTitleType().equals("tvMiniSeries")));

        PCollection<KV<String, BasicInfo>> seriesById = allTvSeries
                .apply("Map show info over Id", WithKeys.of(BasicInfo::getTConst))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(BasicInfo.class)));


        PCollection<ShowData> showData =
                KeyedPCollectionTuple.of(basicTag, seriesById).and(episodeTag, episodesByParents)
                        .apply("Join shows with episodes", CoGroupByKey.create())
                        .apply("Construct ShowData", ParDo.of(new DoFn<KV<String, CoGbkResult>, ShowData>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                KV<String, CoGbkResult> e = c.element();
                                BasicInfo basic = e.getValue().getOnly(basicTag, null);
                                if (basic != null) {
                                    ShowData.Builder builder = new ShowData.Builder(basic);
                                    for (EpisodeData eData : e.getValue().getAll(episodeTag)) {
                                        builder.addEpisode(eData);
                                    }
                                    ShowData showData = builder.build();
                                    c.output(showData);
                                }
                            }
                        }));

        // TODO: Read in old shows and only update updated and new ones, and delete old.
        showData.apply("Creating Show Entities", ParDo.of(new BuildShowDataFn(options.getShowEntity())))
                .apply("Write shows", DatastoreIO.v1().write().withProjectId(options.getDatastoreProject()));

        // TODO: Read in old episodes and only update updated and new ones, and delete old.
        showData.apply("Creating Episode Entities",
                ParDo.of(new BuildEpisodeDataFn(options.getEpisodeEntity(), options.getShowEntity())))
                .apply("Write episodes", DatastoreIO.v1().write().withProjectId(options.getDatastoreProject()));

        /*
          Create the search space
         */
        // TODO: Change search to remove non-alphabetic signs.
        // TODO: Change search to lower case all words.
        PCollection<SearchData> IdsByWord = allTvSeries
                .apply("Create lookup for titles", ParDo.of(new DoFn<BasicInfo, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String id = c.element().getTConst();
                        String[] words = c.element().getPrimaryTitle().split("\\s+");
                        for (String word : words) {
                            c.output(KV.of(word, id));
                        }
                    }
                }))
                .apply("Group by key word", GroupByKey.create())
                .apply("Create POJOs for search space", ParDo.of(new DoFn<KV<String, Iterable<String>>, SearchData>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {

                        SearchData.Builder builder = new SearchData.Builder(c.element().getKey());
                        builder.addTitles(c.element().getValue());
                        c.output(builder.build());
                    }
                }));

        // TODO: Remove POJO step and create entity directly.
        // TODO: Read in old searches and only update the new ones.
        IdsByWord.apply("Create Search Entity", ParDo.of(new BuildSearchDataFn(options.getSearchEntity())))
                .apply("Write searches", DatastoreIO.v1().write().withProjectId(options.getDatastoreProject()));

        p.run();
    }


}
