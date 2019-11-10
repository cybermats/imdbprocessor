package org.cybermats.info;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.cybermats.helpers.InfoHelper;
import org.cybermats.helpers.TSVRow;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

@SuppressWarnings("unused")
@DefaultCoder(AvroCoder.class)
public class BasicInfo implements Serializable {
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
    private final Float rating;
    private BasicInfo() {
        tConst = null;
        titleType = null;
        primaryTitle = null;
        startYear = null;
        endYear = null;
        genres = null;
        rating = null;
    }

    private BasicInfo(
            String tConst,
            String titleType,
            String primaryTitle,
            Integer startYear,
            Integer endYear,
            String[] genres,
            Float rating
    ) {
        this.tConst = tConst;
        this.titleType = titleType;
        this.primaryTitle = primaryTitle;
        this.startYear = startYear;
        this.endYear = endYear;
        this.genres = genres;
        this.rating = rating;
    }

    public static BasicInfo of(TSVRow row) {
        return new BasicInfo.Builder(row).build();
    }

    public Builder getBuilder() {
        return new Builder(tConst, titleType, primaryTitle, startYear, endYear, genres, rating);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicInfo basicInfo = (BasicInfo) o;

        if (!Objects.equals(tConst, basicInfo.tConst)) return false;
        if (!Objects.equals(titleType, basicInfo.titleType)) return false;
        if (!Objects.equals(primaryTitle, basicInfo.primaryTitle))
            return false;
        if (!Objects.equals(startYear, basicInfo.startYear)) return false;
        if (!Objects.equals(endYear, basicInfo.endYear)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(genres, basicInfo.genres)) return false;
        return Objects.equals(rating, basicInfo.rating);
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

    public String[] getGenres() {
        return genres;
    }

    public Float getRating() {
        return rating;
    }

    @Override
    public int hashCode() {
        int result = (tConst != null ? tConst.hashCode() : 0);
        result = 31 * result + (titleType != null ? titleType.hashCode() : 0);
        result = 31 * result + (primaryTitle != null ? primaryTitle.hashCode() : 0);
        result = 31 * result + (startYear != null ? startYear.hashCode() : 0);
        result = 31 * result + (endYear != null ? endYear.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(genres);
        result = 31 * result + (rating != null ? rating.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BasicInfo{" +
                "tConst='" + tConst + '\'' +
                ", titleType='" + titleType + '\'' +
                ", primaryTitle='" + primaryTitle + '\'' +
                ", startYear=" + startYear +
                ", endYear=" + endYear +
                ", genres=" + Arrays.toString(genres) +
                ", rating=" + rating +
                '}';
    }

    public static class Builder {
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

        public Builder(TSVRow row) {
            this.tConst = InfoHelper.parseString(row.get("tconst"));
            this.titleType = InfoHelper.parseString(row.get("titleType"));
            this.primaryTitle = InfoHelper.parseString(row.get("primaryTitle"));
            this.startYear = InfoHelper.parseInt(row.get("startYear"));
            this.endYear = InfoHelper.parseInt(row.get("endYear"));
            this.genres = InfoHelper.parseStringArray(row.get("genres"));
        }

        public Builder(
                String tConst,
                String titleType,
                String primaryTitle,
                Integer startYear,
                Integer endYear,
                String[] genres,
                Float rating
        ) {
            this.tConst = tConst;
            this.titleType = titleType;
            this.primaryTitle = primaryTitle;
            this.startYear = startYear;
            this.endYear = endYear;
            this.genres = genres;
            this.rating = rating;
        }

        public Builder(String tConst) {
            this.tConst = tConst;
        }

        public Builder setPrimaryTitle(String primaryTitle) {
            this.primaryTitle = primaryTitle;
            return this;
        }

        public Builder setRating(Float rating) {
            this.rating = rating;
            return this;
        }

        public BasicInfo build() {
            return new BasicInfo(
                    tConst, titleType, primaryTitle,
                    startYear, endYear, genres, rating
            );
        }

    }
}
