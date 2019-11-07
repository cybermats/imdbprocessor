package org.cybermats;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ParseTSVFn extends DoFn<String, TSVRow> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseTSVFn.class);
    private final ValueProvider<String[]> headerValues;

    ParseTSVFn(ValueProvider<String[]> headers) {
        this.headerValues = headers;
    }

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<TSVRow> receiver) {
        String[] words = element.split("\t");
        String[] headers = this.headerValues.get();
        if (words.length != headers.length) {
            LOG.error("Bad format on input. Actual columns: {}, expected columns: {}.", words.length, headers.length);
            return;
        }
        TSVRow row = new TSVRow();
        for (int i = 0; i < words.length; i++) {
            if (headers[i].equals(words[i]))
                return;
            if (words[i].equals("\\N"))
                row.put(headers[i], null);
            else
                row.put(headers[i], words[i].trim());
        }
        receiver.output(row);
    }
}
