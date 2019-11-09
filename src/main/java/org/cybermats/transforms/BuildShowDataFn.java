package org.cybermats.transforms;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.cybermats.data.ShowData;
import org.cybermats.helpers.EntityHelper;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;

public class BuildShowDataFn extends DoFn<ShowData, Entity> {
    private final ValueProvider<String> showKind;
    private final ValueProvider<String> projectId;

    public BuildShowDataFn(ValueProvider<String> showKind,
                           ValueProvider<String> projectId) {
        this.showKind = showKind;
        this.projectId = projectId;
    }

    @ProcessElement
    public void processElement(@Element ShowData show, OutputReceiver<Entity> receiver) {
        String id = show.getTConst();
        Key.Builder keyBuilder = makeKey(showKind.get(), id);
        PartitionId.Builder partitionBuilder = keyBuilder.getPartitionIdBuilder();
        partitionBuilder.setProjectId(projectId.get());
        keyBuilder.setPartitionId(partitionBuilder.build());
        Key key = keyBuilder.build();


        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("primaryTitle",
                EntityHelper.createValue(show.getPrimaryTitle()));
        entityBuilder.putProperties("startYear",
                EntityHelper.createValue(show.getStartYear()));
        entityBuilder.putProperties("endYear",
                EntityHelper.createValue(show.getEndYear()));
        entityBuilder.putProperties("rating",
                EntityHelper.createValue(show.getRating()));

        Entity entity = entityBuilder.build();
        receiver.output(entity);
    }
}
