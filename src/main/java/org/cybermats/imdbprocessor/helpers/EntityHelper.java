package org.cybermats.imdbprocessor.helpers;

import com.google.datastore.v1.Value;
import com.google.protobuf.NullValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

public class EntityHelper {

    public static Value createValue(String value) {
        if (value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).setExcludeFromIndexes(true).build();
        }
        return makeValue(value).setExcludeFromIndexes(true).build();
    }

    public static Value createValue(Integer value) {
        if (value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).setExcludeFromIndexes(true).build();
        }
        return makeValue(value).setExcludeFromIndexes(true).build();
    }

    public static Value createValue(Float value) {
        if (value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).setExcludeFromIndexes(true).build();
        }
        return makeValue(value).setExcludeFromIndexes(true).build();
    }

    public static Value createValue(Collection<String> values) {
        if (values == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).setExcludeFromIndexes(true).build();
        }

        List<Value> vs = new ArrayList<>();
        for (String value : values) {
            vs.add(createValue(value));
        }
        return makeValue(vs).build();
    }


}
