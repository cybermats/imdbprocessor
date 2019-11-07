package org.cybermats;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FilterPredicate implements SerializableFunction<TSVRow, Boolean> {
    private static final Logger LOG = LoggerFactory.getLogger(FilterPredicate.class);
    private final ValueProvider<String> filterColumn;
    private final ValueProvider<String[]> filterValues;

    FilterPredicate(ValueProvider<String> column, ValueProvider<String[]> values) {
        this.filterColumn = column;
        this.filterValues = values;
    }

    @Override
    public Boolean apply(TSVRow input) {
        if (this.filterColumn == null) {
            return true;
        }
        String filterColumn = this.filterColumn.get();
        String[] filterValues = this.filterValues.get();

        String value = input.get(filterColumn);
        for (String filterValue : filterValues) {
            if (filterValue.equals(value)) {
                return true;
            }
        }
        return false;
    }
}
