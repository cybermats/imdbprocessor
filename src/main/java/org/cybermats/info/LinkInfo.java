package org.cybermats.info;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.cybermats.helpers.InfoHelper;
import org.cybermats.helpers.TSVRow;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class LinkInfo implements Serializable {
    @Nullable
    private final String tConst;
    @Nullable
    private final String parentTConst;
    @Nullable
    private final Integer seasonNumber;
    @Nullable
    private final Integer episodeNumber;

    public LinkInfo() {
        tConst = null;
        parentTConst = null;
        seasonNumber = null;
        episodeNumber = null;
    }

    public LinkInfo(TSVRow row) {
        this.tConst = InfoHelper.parseString(row.get("tconst"));
        this.parentTConst = InfoHelper.parseString(row.get("parentTconst"));
        this.seasonNumber = InfoHelper.parseInt(row.get("seasonNumber"));
        this.episodeNumber = InfoHelper.parseInt(row.get("episodeNumber"));
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
}
