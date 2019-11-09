package org.cybermats.helpers;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class ValueProviderHelper {
    public static ValueProvider.NestedValueProvider<String, String> createFilename(
            ValueProvider<String> directory, String filename) {
        return ValueProvider.NestedValueProvider.of(
                directory, (SerializableFunction<String, String>) dir -> {
                    String path = dir;
                    if (!path.endsWith("/")) {
                        path += "/";
                    }
                    path += filename;
                    return path;
                }
        );
    }

}
