package org.cybermats.data;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.cybermats.info.BasicInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class ShowData implements Serializable {
    @Nullable
    private String tConst;
    @Nullable
    private String primaryTitle;
    @Nullable
    private Integer startYear;
    @Nullable
    private Integer endYear;
    @Nullable
    private Float rating;
    @Nullable
    private List<EpisodeData> episodes = null;

    private ShowData() {
    }

    public String getTConst() {
        return tConst;
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

    public Float getRating() {
        return rating;
    }

    public List<EpisodeData> getEpisodes() {
        return episodes;
    }

    static public class Builder {
        private final String tConst;
        private final String primaryTitle;
        private final Integer startYear;
        private final Integer endYear;
        private final Float rating;
        private final List<EpisodeData> episodes = new ArrayList<>();

        public Builder(BasicInfo b) {
            this.tConst = b.getTConst();
            this.primaryTitle = b.getPrimaryTitle();
            this.startYear = b.getStartYear();
            this.endYear = b.getEndYear();
            this.rating = b.getRating();
        }

        @SuppressWarnings("UnusedReturnValue")
        public Builder addEpisode(EpisodeData e) {
            episodes.add(e);
            return this;
        }

        public ShowData build() {
            ShowData data = new ShowData();
            data.tConst = this.tConst;
            data.primaryTitle = this.primaryTitle;
            data.startYear = this.startYear;
            data.endYear = this.endYear;
            data.rating = this.rating;
            data.episodes = Collections.unmodifiableList(this.episodes);
            return data;
        }

    }
}
