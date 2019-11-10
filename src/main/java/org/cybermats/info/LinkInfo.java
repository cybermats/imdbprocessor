package org.cybermats.info;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.cybermats.helpers.InfoHelper;
import org.cybermats.helpers.TSVRow;

import java.io.Serializable;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class LinkInfo implements Serializable {
    private LinkInfo() {
        tConst = null;
        parentTConst = null;
        seasonNumber = null;
        episodeNumber = null;
    }


    @Nullable
    private final String tConst;
    @Nullable
    private final String parentTConst;
    @Nullable
    private final Integer seasonNumber;
    @Nullable
    private final Integer episodeNumber;

    private LinkInfo(String tConst, String parentTConst, Integer seasonNumber, Integer episodeNumber) {
        this.tConst = tConst;
        this.parentTConst = parentTConst;
        this.seasonNumber = seasonNumber;
        this.episodeNumber = episodeNumber;
    }

    public static LinkInfo of(TSVRow row) {
        return new LinkInfo.Builder(row).build();
    }

    @SuppressWarnings("unused")
    public Builder getBuilder() {
        return new Builder(tConst, parentTConst, seasonNumber, episodeNumber);
    }

    @Override
    public String toString() {
        return "LinkInfo{" +
                "tConst='" + tConst + '\'' +
                ", parentTConst='" + parentTConst + '\'' +
                ", seasonNumber=" + seasonNumber +
                ", episodeNumber=" + episodeNumber +
                '}';
    }

    public String getTConst() {
        return tConst;
    }

    public String getParentTConst() {
        return parentTConst;
    }

    public Integer getSeasonNumber() {
        return seasonNumber;
    }

    public Integer getEpisodeNumber() {
        return episodeNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LinkInfo linkInfo = (LinkInfo) o;

        if (!Objects.equals(tConst, linkInfo.tConst)) return false;
        if (!Objects.equals(parentTConst, linkInfo.parentTConst))
            return false;
        if (!Objects.equals(seasonNumber, linkInfo.seasonNumber))
            return false;
        return Objects.equals(episodeNumber, linkInfo.episodeNumber);
    }

    @Override
    public int hashCode() {
        int result = tConst != null ? tConst.hashCode() : 0;
        result = 31 * result + (parentTConst != null ? parentTConst.hashCode() : 0);
        result = 31 * result + (seasonNumber != null ? seasonNumber.hashCode() : 0);
        result = 31 * result + (episodeNumber != null ? episodeNumber.hashCode() : 0);
        return result;
    }

    @SuppressWarnings("unused")
    public static class Builder {
        private String tConst;
        private String parentTConst;
        private Integer seasonNumber;
        private Integer episodeNumber;

        public Builder() {
        }

        Builder(String tConst, String parentTConst, Integer seasonNumber, Integer episodeNumber) {
            this.tConst = tConst;
            this.parentTConst = parentTConst;
            this.seasonNumber = seasonNumber;
            this.episodeNumber = episodeNumber;
        }

        Builder(TSVRow row) {
            this.tConst = InfoHelper.parseString(row.get("tconst"));
            this.parentTConst = InfoHelper.parseString(row.get("parentTconst"));
            this.seasonNumber = InfoHelper.parseInt(row.get("seasonNumber"));
            this.episodeNumber = InfoHelper.parseInt(row.get("episodeNumber"));
        }

        LinkInfo build() {
            return new LinkInfo(tConst, parentTConst, seasonNumber, episodeNumber);
        }
    }
}
