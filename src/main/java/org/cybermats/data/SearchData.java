package org.cybermats.data;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class SearchData implements Serializable {
    private String keyWord;
    private List<String> titles;

    private SearchData() {
    }

    public String getKeyWord() {
        return keyWord;
    }

    public List<String> getTitles() {
        return titles;
    }

    public static class Builder {
        private final String keyWord;
        private final List<String> titles = new ArrayList<>();

        public Builder(String keyword) {
            this.keyWord = keyword;
        }

        @SuppressWarnings("UnusedReturnValue")
        public Builder addTitles(Iterable<String> titles) {
            titles.forEach(this.titles::add);
            return this;
        }

        public SearchData build() {
            SearchData data = new SearchData();
            data.keyWord = this.keyWord;
            data.titles = Collections.unmodifiableList(this.titles);
            return data;
        }

    }
}
