package org.cybermats.imdbprocessor.composites;

import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

public class FilterChanged extends PTransform<PCollectionTuple, PCollection<Entity>> {
    private final TupleTag<Entity> newSearchTag = new TupleTag<Entity>() {
    };
    private final TupleTag<Entity> oldSearchTag = new TupleTag<Entity>() {
    };

    public TupleTag<Entity> getNewSearchTag() {
        return newSearchTag;
    }

    public TupleTag<Entity> getOldSearchTag() {
        return oldSearchTag;
    }

    @Override
    public PCollection<Entity> expand(PCollectionTuple inputs) {
        PCollection<Entity> newSearches = inputs.get(newSearchTag);
        PCollection<Entity> oldSearches = inputs.get(oldSearchTag);

        PCollection<KV<String, Entity>> newSearchesById = newSearches
                .apply("Map up new entities", WithKeys.of(new ExtractEntityKey()));
        PCollection<KV<String, Entity>> oldSearchesById = oldSearches
                .apply("Map up old entities", WithKeys.of(new ExtractEntityKey()));

        return KeyedPCollectionTuple.of(newSearchTag, newSearchesById).and(oldSearchTag, oldSearchesById)
                .apply("Join old and new entities", CoGroupByKey.create())
                .apply("Filter out identical entities ", ParDo.of(new DoFn<KV<String, CoGbkResult>, Entity>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();
                        Entity oldSearch = e.getValue().getOnly(oldSearchTag, null);
                        Entity newSearch = e.getValue().getOnly(newSearchTag, null);
                        if (newSearch != null) {
                            if (!newSearch.equals(oldSearch)) {
                                c.output(newSearch);
                            }
                        }
                    }
                }));
    }

    private static class ExtractEntityKey implements SerializableFunction<Entity, String> {

        @Override
        public String apply(Entity input) {
            int paths = input.getKey().getPathCount();
            return input.getKey().getPath(paths - 1).getName();
        }
    }

}