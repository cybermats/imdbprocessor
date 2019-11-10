package org.cybermats;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Query;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.cybermats.composites.CreateBasicsWithRatings;
import org.cybermats.composites.CreateSearchSpace;
import org.cybermats.composites.FilterChanged;
import org.cybermats.data.EpisodeData;
import org.cybermats.data.ShowData;
import org.cybermats.info.BasicInfo;
import org.cybermats.info.LinkInfo;
import org.cybermats.transforms.BuildEpisodeDataFn;
import org.cybermats.transforms.BuildShowDataFn;
import org.cybermats.transforms.ParseLinkInfoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ImdbProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ImdbProcessor.class);

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

        PCollection<String> basicsFile = p.apply("Read Basics File", TextIO.read().from(
                createFilename(options.getInputDir(), "title.basics.tsv.gz")));
        PCollection<String> ratingsFile = p.apply("Read Ratings File",
                TextIO.read().from(createFilename(options.getInputDir(), "title.ratings.tsv.gz")));

        CreateBasicsWithRatings createBasicsWithRatings = new CreateBasicsWithRatings();

        PCollection<BasicInfo> basicsWithRatings = PCollectionTuple
                .of(createBasicsWithRatings.getBasicsFileTag(), basicsFile)
                .and(createBasicsWithRatings.getRatingsFileTag(), ratingsFile)
                .apply("Create basics with Ratings", createBasicsWithRatings);

        /*
         Read the episodes file to get the link to the parent for each episode.
         */

        // Filter out only the episode info.
        PCollection<KV<String, BasicInfo>> allEpisodesById = basicsWithRatings
                .apply("Filter tvEpisodes", Filter.by((BasicInfo b) -> b.getTitleType().equals("tvEpisode")))
                .apply("Map Episodes over Id", WithKeys.of(BasicInfo::getTConst))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(BasicInfo.class)));

        // Read Episode link info into map of POJO objects.
        PCollection<KV<String, LinkInfo>> linksById = p.apply("Read Episode File",
                TextIO.read().from(createFilename(options.getInputDir(), "title.episode.tsv.gz")))
                .apply("Parse TSV info Link Info by id", ParDo.of(new ParseLinkInfoFn()))
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


        /*
        Write back show data
         */

        Query.Builder showDataQueryBuilder = Query.newBuilder();
        showDataQueryBuilder.addKindBuilder().setName("showData");
        Query showDataQuery = showDataQueryBuilder.build();

        PCollection<Entity> oldShowData = p
                .apply("Get old show data", DatastoreIO.v1().read().withQuery(showDataQuery).withProjectId(options.getDatastoreProject()));

        PCollection<Entity> newShowData = showData.apply("Creating Show Entities", ParDo.of(new BuildShowDataFn(
                options.getShowEntity(), options.getDatastoreProject())));

        FilterChanged showDataUpdateCheck = new FilterChanged();
        PCollectionTuple.of(showDataUpdateCheck.getOldSearchTag(), oldShowData)
                .and(showDataUpdateCheck.getNewSearchTag(), newShowData)
                .apply("Filter out unchanged shows", showDataUpdateCheck)
                .apply("Write shows", DatastoreIO.v1().write().withProjectId(options.getDatastoreProject()));


        /*
        Write back episode data
         */
        Query.Builder episodeDataQueryBuilder = Query.newBuilder();
        episodeDataQueryBuilder.addKindBuilder().setName("showData");
        Query episodeDataQuery = episodeDataQueryBuilder.build();

        PCollection<Entity> oldEpisodeData = p
                .apply("Get old episode data", DatastoreIO.v1().read().withQuery(episodeDataQuery).withProjectId(options.getDatastoreProject()));

        PCollection<Entity> newEpisodeData = showData.apply("Creating Episode Entities",
                ParDo.of(new BuildEpisodeDataFn(
                        options.getEpisodeEntity(), options.getShowEntity(), options.getDatastoreProject())));

        FilterChanged episodeDataUpdateCheck = new FilterChanged();
        PCollectionTuple.of(episodeDataUpdateCheck.getOldSearchTag(), oldEpisodeData)
                .and(episodeDataUpdateCheck.getNewSearchTag(), newEpisodeData)
                .apply("Filter out unchanged shows", episodeDataUpdateCheck)
                .apply("Write episodes", DatastoreIO.v1().write().withProjectId(options.getDatastoreProject()));

      /*
          Create the search space
         */

        Query.Builder qb = Query.newBuilder();
        qb.addKindBuilder().setName("search");
        Query q = qb.build();

        PCollection<Entity> oldSearches = p
                .apply(DatastoreIO.v1().read().withQuery(q).withProjectId(options.getDatastoreProject()));

        PCollection<Entity> newSearches = allTvSeries
                .apply("Create Search Space", new CreateSearchSpace(
                        options.getSearchEntity(), options.getDatastoreProject()));

        FilterChanged searchUpdateCheck = new FilterChanged();
        PCollectionTuple.of(searchUpdateCheck.getOldSearchTag(), oldSearches).and(searchUpdateCheck.getNewSearchTag(), newSearches)
                .apply("Filter out unchanged searches", searchUpdateCheck)
                .apply("Write searches", DatastoreIO.v1().write().withProjectId(options.getDatastoreProject()));

        p.run();
    }


}
