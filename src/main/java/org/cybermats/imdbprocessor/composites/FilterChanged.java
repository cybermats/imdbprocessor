package org.cybermats.imdbprocessor.composites;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

public class FilterChanged extends PTransform<PCollectionTuple, PCollectionTuple> {
    private final TupleTag<Entity> newSearchTag = new TupleTag<Entity>() {
    };
    private final TupleTag<Entity> oldSearchTag = new TupleTag<Entity>() {
    };

    private final TupleTag<Entity> deletedEntityTag = new TupleTag<Entity>() {
    };
    private final TupleTag<Entity> upsertEntityTag = new TupleTag<Entity>() {
    };

    public TupleTag<Entity> getNewSearchTag() {
        return newSearchTag;
    }

    public TupleTag<Entity> getOldSearchTag() {
        return oldSearchTag;
    }

    public TupleTag<Entity> getDeletedEntityTag() {
        return deletedEntityTag;
    }

    public TupleTag<Entity> getUpsertEntityTag() {
        return upsertEntityTag;
    }

    @Override
    public PCollectionTuple expand(PCollectionTuple inputs) {
        PCollection<Entity> newSearches = inputs.get(newSearchTag);
        PCollection<Entity> oldSearches = inputs.get(oldSearchTag);

        PCollection<KV<Key, Entity>> newSearchesById = newSearches
                .apply("Map up new entities", WithKeys.of(new ExtractEntityKey()));
        PCollection<KV<Key, Entity>> oldSearchesById = oldSearches
                .apply("Map up old entities", WithKeys.of(new ExtractEntityKey()));

        return KeyedPCollectionTuple.of(newSearchTag, newSearchesById).and(oldSearchTag, oldSearchesById)
                .apply("Join old and new entities", CoGroupByKey.create())
                .apply("Filter out identical entities ", ParDo.of(new DoFn<KV<Key, CoGbkResult>, Entity>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<Key, CoGbkResult> e = c.element();
                        Entity oldSearch = e.getValue().getOnly(oldSearchTag, null);
                        Entity newSearch = e.getValue().getOnly(newSearchTag, null);
                        if (newSearch != null) {
                            if (!newSearch.equals(oldSearch)) {
                                c.output(upsertEntityTag, newSearch);
                            }
                        } else {
                            c.output(deletedEntityTag, oldSearch);
                        }
                    }
                }).withOutputTags(upsertEntityTag, TupleTagList.of(deletedEntityTag)));
    }

    private static class ExtractEntityKey implements SerializableFunction<Entity, Key> {

        @Override
        public Key apply(Entity input) {
            return input.getKey();
        }
    }

}