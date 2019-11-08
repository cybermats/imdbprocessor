package org.cybermats.info;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.cybermats.helpers.InfoHelper;
import org.cybermats.helpers.TSVRow;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class RatingInfo implements Serializable {

    @Nullable
    private final String tConst;
    @Nullable
    private final Float rating;

    public RatingInfo(TSVRow row) throws NumberFormatException {
        this.tConst = InfoHelper.parseString(row.get("tconst"));
        this.rating = InfoHelper.parseFloat(row.get("rating"));
    }

    public String getTConst() {
        return tConst;
    }

    public Float getRating() {
        return rating;
    }

}
