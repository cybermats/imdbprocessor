package org.cybermats.transforms;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.cybermats.data.SearchData;
import org.cybermats.helpers.EntityHelper;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;

public class BuildSearchDataFn extends DoFn<SearchData, Entity> {
    private final ValueProvider<String> searchKind;

    public BuildSearchDataFn(ValueProvider<String> searchKind) {
        this.searchKind = searchKind;
    }

    @ProcessElement
    public void processElement(@Element SearchData search, OutputReceiver<Entity> receiver) {
        String id = search.getKeyWord();
        Key key = makeKey(searchKind.get(), id).build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("titles", EntityHelper.createValue(search.getTitles()));

        receiver.output(entityBuilder.build());
    }

}
