package org.cybermats.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.cybermats.info.BasicInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchGeneratorFn extends DoFn<BasicInfo, KV<String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(SearchGeneratorFn.class);

    @ProcessElement
    public void processElement(@Element BasicInfo element, OutputReceiver<KV<String, String>> receiver) {
        String id = element.getTConst();
        String title = element.getPrimaryTitle().toLowerCase();
        LOG.info(title);
        title = title.replaceAll("[^\\p{IsLatin}]", " ");
        LOG.info(title);
        String[] words = title.split("\\s+");
        for (String word : words) {
            if (word.length() > 0)
                receiver.output(KV.of(word, id));
        }
    }
}
