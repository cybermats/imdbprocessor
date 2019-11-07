package org.cybermats;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.NullValue;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

class BuildEntityFn extends DoFn<TSVRow, Entity> {
    private static final Logger LOG = LoggerFactory.getLogger(BuildEntityFn.class);

    private final ValueProvider<String> kind;
    private final ValueProvider<String[]> properties;
    private final ValueProvider<String[]> propertyTypes;
    private final ValueProvider<String> idHeader;

    BuildEntityFn(ValueProvider<String> kind,
                  ValueProvider<String[]> properties,
                  ValueProvider<String[]> propertyTypes,
                  ValueProvider<String> idHeader) {
        this.kind = kind;
        this.properties = properties;
        this.propertyTypes = propertyTypes;
        this.idHeader = idHeader;
    }

    private static Value createString(String str) {
        if (str == null) return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        return makeValue(str).setExcludeFromIndexes(true).build();
    }

    private static Value createInt(String str) {
        if (str == null) return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        int value = Integer.parseInt(str);
        return makeValue(value).setExcludeFromIndexes(true).build();
    }

    private static Value createFloat(String str) {
        if (str == null) return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
        float value = Float.parseFloat(str);
        return makeValue(value).setExcludeFromIndexes(true).build();
    }

    private static Entity makeEntity(TSVRow element,
                                     String kind,
                                     String[] properties,
                                     String[] propertyTypes,
                                     String idHeader) {

        String id = element.get(idHeader);
        Key key = makeKey(kind, id).build();
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);

        for (int i = 0; i < properties.length; i++) {
            try {
                Value value;
                String propertyValue = element.get(properties[i]);
                switch (propertyTypes[i]) {
                    case "int":
                        value = createInt(propertyValue);
                        break;
                    case "float":
                        value = createFloat(propertyValue);
                        break;
                    default:
                        value = createString(propertyValue);
                        break;
                }
                entityBuilder.putProperties(properties[i], value);
            } catch (NumberFormatException ex) {
                LOG.error("Couldn't create an entity of a certain type.", ex);
            }

        }
        return entityBuilder.build();
    }

    @ProcessElement
    public void processElement(@Element TSVRow element, OutputReceiver<Entity> receiver) {
        final String kind = this.kind.get();
        final String[] properties = this.properties.get();
        final String[] propertyTypes = this.propertyTypes.get();
        final String idHeader = this.idHeader.get();
        if (properties.length != propertyTypes.length) {
            LOG.error("properties and propertyTypes does not match.");
            return;
        }
        receiver.output(makeEntity(element, kind, properties, propertyTypes, idHeader));
    }
}
