package org.cybermats.helpers;

import com.google.datastore.v1.Value;
import com.google.protobuf.NullValue;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

class EntityHelperTest {

    @Test
    void createStringValue() {
        Object expected = makeValue("hello").setExcludeFromIndexes(true).build();

        String input = "hello";
        Value actual = EntityHelper.createValue(input);

        Assert.assertEquals(expected, actual);
    }

    @Test
    void createStringNullValue() {
        Object expected = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).setExcludeFromIndexes(true).build();

        String input = null;
        @SuppressWarnings("ConstantConditions") Value actual = EntityHelper.createValue(input);

        Assert.assertEquals(expected, actual);
    }

    @Test
    void createIntValue() {
        Object expected = makeValue(42).setExcludeFromIndexes(true).build();

        Integer input = 42;
        Value actual = EntityHelper.createValue(input);

        Assert.assertEquals(expected, actual);
    }

    @Test
    void createIntNullValue() {
        Object expected = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).setExcludeFromIndexes(true).build();

        Integer input = null;
        Value actual = EntityHelper.createValue(input);

        Assert.assertEquals(expected, actual);
    }

    @Test
    void createFloatValue() {
        Object expected = makeValue(42.0f).setExcludeFromIndexes(true).build();

        Float input = 42.0f;
        Value actual = EntityHelper.createValue(input);

        Assert.assertEquals(expected, actual);
    }

    @Test
    void createFloatNullValue() {
        Object expected = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).setExcludeFromIndexes(true).build();

        Float input = null;
        Value actual = EntityHelper.createValue(input);

        Assert.assertEquals(expected, actual);
    }

    @Test
    void createStringArrayValue() {
        Value item = makeValue("hello").setExcludeFromIndexes(true).build();
        Value expected = makeValue(Collections.singletonList(item)).build();

        List<String> input = Collections.singletonList("hello");
        Value actual = EntityHelper.createValue(input);

        Assert.assertEquals(expected, actual);
    }

    @Test
    void createStringNullArrayValue() {
        Object expected = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).setExcludeFromIndexes(true).build();

        List<String> input = null;
        @SuppressWarnings("ConstantConditions") Value actual = EntityHelper.createValue(input);

        Assert.assertEquals(expected, actual);
    }

    @Test
    void createStringArrayOfNullsValue() {
        Value item = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).setExcludeFromIndexes(true).build();
        Value expected = makeValue(Collections.singletonList(item)).build();

        List<String> input = Arrays.asList(new String[]{null});
        Value actual = EntityHelper.createValue(input);

        Assert.assertEquals(expected, actual);
    }


}