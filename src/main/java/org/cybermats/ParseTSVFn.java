package org.cybermats;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

class ParseTSVFn extends DoFn<String, HashMap<String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseTSVFn.class);
    private final ValueProvider<String[]> headerValues;

    ParseTSVFn(ValueProvider<String[]> headers) {
        this.headerValues = headers;
    }

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<HashMap<String, String>> receiver) {
        String[] words = element.split("\t");
        String[] headers = this.headerValues.get();
        if (words.length != headers.length) {
            LOG.error("Bad format on input. Actual columns: {}, expected columns: {}.", words.length, headers.length);
            return;
        }
        HashMap<String, String> map = new HashMap<>();
        for (int i = 0; i < words.length; i++) {
            if (headers[i].equals(words[i]))
                return;
            if (words[i].equals("\\N"))
                map.put(headers[i], null);
            else
                map.put(headers[i], words[i].trim());
        }
        receiver.output(map);
    }
}
