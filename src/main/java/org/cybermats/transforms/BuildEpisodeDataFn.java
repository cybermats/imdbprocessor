package org.cybermats.transforms;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.cybermats.data.EpisodeData;
import org.cybermats.data.ShowData;
import org.cybermats.helpers.EntityHelper;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;

public class BuildEpisodeDataFn extends DoFn<ShowData, Entity> {
    private final ValueProvider<String> episodeKind;
    private final ValueProvider<String> showKind;

    public BuildEpisodeDataFn(ValueProvider<String> episodeKind, ValueProvider<String> showKind) {
        this.episodeKind = episodeKind;
        this.showKind = showKind;
    }

    @ProcessElement
    public void processElement(@Element ShowData show, OutputReceiver<Entity> receiver) {
        for (EpisodeData episode : show.getEpisodes()) {
            Key key = makeKey(episodeKind.get(), episode.getTConst(),
                    showKind.get(), episode.getParentTConst()).build();
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
