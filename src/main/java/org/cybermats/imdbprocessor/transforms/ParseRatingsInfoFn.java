package org.cybermats.imdbprocessor.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.cybermats.imdbprocessor.helpers.TSVRow;
import org.cybermats.imdbprocessor.info.RatingInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseRatingsInfoFn extends DoFn<String, KV<String, RatingInfo>> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseRatingsInfoFn.class);

    private static final String[] RATING_HEADERS = new String[]{"tconst", "averageRating", "numVotes"};

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<KV<String, RatingInfo>> receiver) {
        String[] words = element.split("\t");
        if (words.length != RATING_HEADERS.length) {
            LOG.error("Bad format on input. Actual columns: {}, expected columns: {}.", words.length, RATING_HEADERS.length);
            return;
        }
        TSVRow row = new TSVRow();

        for (int i = 0; i < words.length; i++) {
            if (RATING_HEADERS[i].equals(words[i]))
                return;
            if (words[i].equals("\\N"))
                row.put(RATING_HEADERS[i], null);
            else
                row.put(RATING_HEADERS[i], words[i].trim());
        }

        RatingInfo basicInfo = RatingInfo.of(row);

        receiver.output(KV.of(basicInfo.getTConst(), basicInfo));
    }
}
