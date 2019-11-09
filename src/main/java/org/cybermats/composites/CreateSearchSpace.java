package org.cybermats.composites;

import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.cybermats.data.SearchData;
import org.cybermats.info.BasicInfo;
import org.cybermats.transforms.BuildSearchDataFn;
import org.cybermats.transforms.SearchGeneratorFn;


public class CreateSearchSpace extends PTransform<PCollection<BasicInfo>, PCollection<Entity>> {
    final ValueProvider<String> searchKind;

    public CreateSearchSpace(ValueProvider<String> searchKind) {
        this.searchKind = searchKind;
    }

    @Override
    public PCollection<Entity> expand(PCollection<BasicInfo> tvSeries) {
        // TODO: Remove POJO step and create entity directly.
        return tvSeries.apply("Create lookup for titles", ParDo.of(new SearchGeneratorFn()))
                .apply("Group by key word", GroupByKey.create())
                .apply("Create POJOs for search space", ParDo.of(new DoFn<KV<String, Iterable<String>>, SearchData>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {

                        SearchData.Builder builder = new SearchData.Builder(c.element().getKey());
                        builder.addTitles(c.element().getValue());
                        c.output(builder.build());
                    }
                })).apply("Create Search Entity", ParDo.of(new BuildSearchDataFn(searchKind)));
    }
}
