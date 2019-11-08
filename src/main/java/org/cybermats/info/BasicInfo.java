package org.cybermats.info;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.cybermats.helpers.InfoHelper;
import org.cybermats.helpers.TSVRow;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class BasicInfo implements Serializable {

    @Nullable
    private final String tConst;
    @Nullable
    private final String titleType;
    @Nullable
    private final String primaryTitle;
    @Nullable
    private final Integer startYear;
    @Nullable
    private final Integer endYear;
    @Nullable
    private final String[] genres;
    @Nullable
    private Float rating;

    public BasicInfo() {
        tConst = null;
        titleType = null;
        primaryTitle = null;
        startYear = null;
        endYear = null;
        genres = null;
    }

    public BasicInfo(TSVRow row) throws NumberFormatException {
        this.tConst = InfoHelper.parseString(row.get("tconst"));
        this.titleType = InfoHelper.parseString(row.get("titleType"));
        this.primaryTitle = InfoHelper.parseString(row.get("primaryTitle"));
        this.startYear = InfoHelper.parseInt(row.get("startYear"));
        this.endYear = InfoHelper.parseInt(row.get("endYear"));
        this.genres = InfoHelper.parseStringArray(row.get("genres"));
    }

    public String getTConst() {
        return tConst;
    }

    public String getTitleType() {
        return titleType;
    }

    public String getPrimaryTitle() {
        return primaryTitle;
    }

    public Integer getStartYear() {
        return startYear;
    }

    public Integer getEndYear() {
        return endYear;
    }

    @SuppressWarnings("unused")
    public String[] getGenres() {
        return genres;
    }

    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }
}
