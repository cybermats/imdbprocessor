package org.cybermats.imdbprocessor.transforms;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.cybermats.imdbprocessor.data.EpisodeData;
import org.cybermats.imdbprocessor.data.ShowData;
import org.cybermats.imdbprocessor.helpers.EntityHelper;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;

public class BuildEpisodeDataFn extends DoFn<ShowData, Entity> {
    private final ValueProvider<String> episodeKind;
    private final ValueProvider<String> showKind;
    private final ValueProvider<String> projectId;

    public BuildEpisodeDataFn(ValueProvider<String> episodeKind,
                              ValueProvider<String> showKind,
                              ValueProvider<String> projectId) {
        this.episodeKind = episodeKind;
        this.showKind = showKind;
        this.projectId = projectId;
    }

    @ProcessElement
    public void processElement(@Element ShowData show, OutputReceiver<Entity> receiver) {
        for (EpisodeData episode : show.getEpisodes()) {
            Key.Builder keyBuilder = makeKey(showKind.get(), episode.getParentTConst(),
                    episodeKind.get(), episode.getTConst());
            PartitionId.Builder partitionBuilder = keyBuilder.getPartitionIdBuilder();
            partitionBuilder.setProjectId(projectId.get());
            keyBuilder.setPartitionId(partitionBuilder.build());
            Key key = keyBuilder.build();
            Entity.Builder entityBuilder = Entity.newBuilder();
            entityBuilder.setKey(key);
            entityBuilder.putProperties("primaryTitle",
                    EntityHelper.createValue(episode.getPrimaryTitle()));
            entityBuilder.putProperties("season",
                    EntityHelper.createValue(episode.getSeason()));
            entityBuilder.putProperties("episode",
                    EntityHelper.createValue(episode.getEpisode()));
            entityBuilder.putProperties("rating",
                    EntityHelper.createValue(episode.getRating()));
            Entity entity = entityBuilder.build();
            receiver.output(entity);
        }
    }

}
