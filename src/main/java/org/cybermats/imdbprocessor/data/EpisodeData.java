package org.cybermats.imdbprocessor.data;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.cybermats.imdbprocessor.info.BasicInfo;
import org.cybermats.imdbprocessor.info.LinkInfo;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class EpisodeData implements Serializable, Comparable<EpisodeData> {
    @Nullable
    private String tConst;
    @Nullable
    private String parentTConst;
    @Nullable
    private String primaryTitle;
    @Nullable
    private Integer season;
    @Nullable
    private Integer episode;
    @Nullable
    private Float rating;

    private EpisodeData() {
        tConst = null;
        parentTConst = null;
        primaryTitle = null;
        season = null;
        episode = null;
        rating = null;
    }

    public String getTConst() {
        return tConst;
    }

    public String getParentTConst() {
        return parentTConst;
    }

    public String getPrimaryTitle() {
        return primaryTitle;
    }

    public Integer getSeason() {
        return season;
    }

    public Integer getEpisode() {
        return episode;
    }

    public Float getRating() {
        return rating;
    }

    @Override
    public int compareTo(EpisodeData episodeData) {
        return this.tConst.compareTo(episodeData.tConst);
    }

    public static class Builder {
        private String tConst = null;
        private String parentTConst = null;
        private String primaryTitle = null;
        private Integer season = null;
        private Integer episode = null;
        private Float rating = null;


        public Builder addBasicInfo(BasicInfo b) {
            this.tConst = b.getTConst();
            this.primaryTitle = b.getPrimaryTitle();
            this.rating = b.getRating();
            return this;
        }

        public Builder addLinkInfo(LinkInfo link) {
            this.parentTConst = link.getParentTConst();
            this.season = link.getSeasonNumber();
            this.episode = link.getEpisodeNumber();
            return this;
        }

        public EpisodeData build() {
            EpisodeData data = new EpisodeData();
            data.tConst = this.tConst;
            data.parentTConst = this.parentTConst;
            data.primaryTitle = this.primaryTitle;
            data.season = this.season;
            data.episode = this.episode;
            data.rating = this.rating;

            return data;
        }

    }
}
