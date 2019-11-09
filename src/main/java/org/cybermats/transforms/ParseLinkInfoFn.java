package org.cybermats.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.cybermats.helpers.TSVRow;
import org.cybermats.info.LinkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseLinkInfoFn extends DoFn<String, KV<String, LinkInfo>> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseLinkInfoFn.class);

    private static final String[] EPISODE_HEADERS = new String[]{"tconst", "parentTconst",
            "seasonNumber", "episodeNumber"};

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<KV<String, LinkInfo>> receiver) {
        String[] words = element.split("\t");
        if (words.length != EPISODE_HEADERS.length) {
            LOG.error("Bad format on input. Actual columns: {}, expected columns: {}.", words.length, EPISODE_HEADERS.length);
            return;
        }
        TSVRow row = new TSVRow();

        for (int i = 0; i < words.length; i++) {
            if (EPISODE_HEADERS[i].equals(words[i]))
                return;
            if (words[i].equals("\\N"))
                row.put(EPISODE_HEADERS[i], null);
            else
                row.put(EPISODE_HEADERS[i], words[i].trim());
        }

        LinkInfo linkInfo = LinkInfo.of(row);

        receiver.output(KV.of(linkInfo.getTConst(), linkInfo));

    }
}