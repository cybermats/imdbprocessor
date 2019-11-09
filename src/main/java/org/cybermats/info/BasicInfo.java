package org.cybermats.info;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.cybermats.helpers.InfoHelper;
import org.cybermats.helpers.TSVRow;

import java.io.Serializable;

@SuppressWarnings("unused")
@DefaultCoder(AvroCoder.class)
public class BasicInfo implements Serializable {
    @Nullable
    private final String tConst;
    @Nullable
    private String titleType;
    @Nullable
    private String primaryTitle;
    @Nullable
    private Integer startYear;
    @Nullable
    private Integer endYear;
    @Nullable
    private String[] genres;
    @Nullable
    private Float rating;

    private BasicInfo() {
        tConst = null;
        titleType = null;
        primaryTitle = null;
        startYear = null;
        endYear = null;
        genres = null;
    }

    private BasicInfo(String tconst) {
        this.tConst = tconst;
        titleType = null;
        primaryTitle = null;
        startYear = null;
        endYear = null;
        genres = null;
    }

    private BasicInfo(TSVRow row) throws NumberFormatException {
        this.tConst = InfoHelper.parseString(row.get("tconst"));
        this.titleType = InfoHelper.parseString(row.get("titleType"));
        this.primaryTitle = InfoHelper.parseString(row.get("primaryTitle"));
        this.startYear = InfoHelper.parseInt(row.get("startYear"));
        this.endYear = InfoHelper.parseInt(row.get("endYear"));
        this.genres = InfoHelper.parseStringArray(row.get("genres"));
    }


    public static BasicInfo of(TSVRow row) {
        return new BasicInfo(row);
    }

    public static BasicInfo of(String tconst) {
        return new BasicInfo(tconst);
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

    public BasicInfo setRating(Float rating) {
        this.rating = rating;
        return this;
    }

    public BasicInfo setTitleType(String titleType) {
        this.titleType = titleType;
        return this;
    }

    public BasicInfo setPrimaryTitle(String primaryTitle) {
        this.primaryTitle = primaryTitle;
        return this;
    }

    public BasicInfo setStartYear(Integer startYear) {
        this.startYear = startYear;
        return this;
    }

    public BasicInfo setEndYear(Integer endYear) {
        this.endYear = endYear;
        return this;
    }

    public BasicInfo setGenres(String[] genres) {
        this.genres = genres;
        return this;
    }


}
