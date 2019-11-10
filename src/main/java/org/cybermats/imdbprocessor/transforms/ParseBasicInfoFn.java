package org.cybermats.imdbprocessor.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.cybermats.imdbprocessor.helpers.TSVRow;
import org.cybermats.imdbprocessor.info.BasicInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseBasicInfoFn extends DoFn<String, KV<String, BasicInfo>> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseBasicInfoFn.class);

    private static final String[] BASIC_HEADERS = new String[]{"tconst", "titleType", "primaryTitle",
            "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genre"};

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<KV<String, BasicInfo>> receiver) {
        String[] words = element.split("\t");
        if (words.length != BASIC_HEADERS.length) {
            LOG.error("Bad format on input. Actual columns: {}, expected columns: {}.", words.length, BASIC_HEADERS.length);
            return;
        }
        TSVRow row = new TSVRow();

        for (int i = 0; i < words.length; i++) {
            if (BASIC_HEADERS[i].equals(words[i]))
                return;
            if (words[i].equals("\\N"))
                row.put(BASIC_HEADERS[i], null);
            else
                row.put(BASIC_HEADERS[i], words[i].trim());
        }

        BasicInfo basicInfo = BasicInfo.of(row);

        receiver.output(KV.of(basicInfo.getTConst(), basicInfo));
    }
}
