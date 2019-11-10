package org.cybermats.imdbprocessor.info;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.cybermats.imdbprocessor.helpers.InfoHelper;
import org.cybermats.imdbprocessor.helpers.TSVRow;

import java.io.Serializable;

@SuppressWarnings("ALL")
@DefaultCoder(AvroCoder.class)
public class RatingInfo implements Serializable {
    private RatingInfo() {
        tConst = null;
        rating = null;
    }

    @Nullable
    private final String tConst;
    @Nullable
    private final Float rating;

    private RatingInfo(String tConst, Float rating) {
        this.tConst = tConst;
        this.rating = rating;
    }

    public static RatingInfo of(TSVRow row) {
        return new RatingInfo.Builder(row).build();
    }

    public Builder getBuilder() {
        return new Builder(tConst, rating);
    }

    @Override
    public String toString() {
        return "RatingInfo{" +
                "tConst='" + tConst + '\'' +
                ", rating=" + rating +
                '}';
    }

    public String getTConst() {
        return tConst;
    }

    public Float getRating() {
        return rating;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RatingInfo that = (RatingInfo) o;

        if (tConst != null ? !tConst.equals(that.tConst) : that.tConst != null) return false;
        return rating != null ? rating.equals(that.rating) : that.rating == null;
    }

    @Override
    public int hashCode() {
        int result = tConst != null ? tConst.hashCode() : 0;
        result = 31 * result + (rating != null ? rating.hashCode() : 0);
        return result;
    }

    public static class Builder {
        private final String tConst;
        private final Float rating;

        public Builder() {
            this.tConst = null;
            this.rating = null;
        }

        Builder(String tConst, Float rating) {
            this.tConst = tConst;
            this.rating = rating;
        }

        Builder(TSVRow row) {
            this.tConst = InfoHelper.parseString(row.get("tconst"));
            this.rating = InfoHelper.parseFloat(row.get("averageRating"));
        }

        RatingInfo build() {
            return new RatingInfo(tConst, rating);
        }

    }
}
