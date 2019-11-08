package org.cybermats.helpers;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.util.HashMap;

public class TSVRow extends HashMap<String, String> {
    @SuppressWarnings("unused")
    public static Coder getCoder() {
        return MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    }
}
