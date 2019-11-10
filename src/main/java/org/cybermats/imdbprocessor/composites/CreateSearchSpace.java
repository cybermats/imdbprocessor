package org.cybermats.imdbprocessor.composites;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.cybermats.imdbprocessor.helpers.EntityHelper;
import org.cybermats.imdbprocessor.info.BasicInfo;
import org.cybermats.imdbprocessor.transforms.SearchGeneratorFn;

import java.util.ArrayList;
import java.util.Collection;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;


public class CreateSearchSpace extends PTransform<PCollection<BasicInfo>, PCollection<Entity>> {
    private final ValueProvider<String> searchKind;
    private final ValueProvider<String> projectId;

    public CreateSearchSpace(ValueProvider<String> searchKind, ValueProvider<String> projectId) {
        this.searchKind = searchKind;
        this.projectId = projectId;
    }

    @Override
    public PCollection<Entity> expand(PCollection<BasicInfo> tvSeries) {
        return tvSeries.apply("Create lookup for titles", ParDo.of(new SearchGeneratorFn()))
                .apply("Group by key word", GroupByKey.create())
                .apply("Create POJOs for search space", ParDo.of(new DoFn<KV<String, Iterable<String>>, Entity>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String term = c.element().getKey();
                        Collection<String> shows = new ArrayList<>();
                        c.element().getValue().forEach(shows::add);
                        Key.Builder keyBuilder = makeKey(searchKind.get(), term);
                        PartitionId.Builder partitionBuilder = keyBuilder.getPartitionIdBuilder();
                        PartitionId pId = partitionBuilder.setProjectId(projectId.get()).build();
                        keyBuilder.setPartitionId(pId);
                        Key key = keyBuilder.build();
                        Entity.Builder entityBuilder = Entity.newBuilder();
                        entityBuilder.setKey(key);
                        entityBuilder.putProperties("titles", EntityHelper.createValue(shows));

                        c.output(entityBuilder.build());
                    }
                }));
    }
}
