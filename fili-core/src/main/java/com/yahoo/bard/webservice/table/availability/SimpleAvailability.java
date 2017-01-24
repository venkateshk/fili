// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An availability which guarantees immutability on its contents.
 */
public class SimpleAvailability implements Availability {

    private final Map<Column, List<Interval>> availabilities;

    /**
     * Constructor.
     *
     * @param map A map of columns to lists of available intervals
     */
    public SimpleAvailability(Map<Column, List<Interval>> map) {
        availabilities = Collections.unmodifiableMap(new HashMap<>(map));
    }

    /**
     * Constructor.
     *
     * @param dimensionIntervals  The dimension availability map by dimension name
     * @param metricIntervals  The metric availability map
     * @param dimensionDictionary  The dictionary to resolve dimension names against
     */
    public SimpleAvailability(
            Map<String, Set<Interval>> dimensionIntervals,
            Map<String, Set<Interval>> metricIntervals,
            DimensionDictionary dimensionDictionary
    ) {
        Function<Entry<String, Set<Interval>>, Column> dimensionKeyMapper =
                entry -> new DimensionColumn(dimensionDictionary.findByApiName(entry.getKey()));
        Function<Entry<String, Set<Interval>>, Column> metricKeyMapper =
                entry -> new MetricColumn(entry.getKey());
        Function<Entry<String, Set<Interval>>, List<Interval>> valueMapper =
                entry -> new SimplifiedIntervalList(entry.getValue());

        Map<Column, List<Interval>> map = dimensionIntervals.entrySet().stream()
                .collect(Collectors.toMap(dimensionKeyMapper, valueMapper));
        map.putAll(
                metricIntervals.entrySet().stream()
                        .collect(Collectors.toMap(metricKeyMapper, valueMapper))
        );

        availabilities = Collections.unmodifiableMap(map);
    }

    @Override
    public List<Interval> get(final Column column) {
        return availabilities.get(column);
    }

    @Override
    public Collection<List<Interval>> values() {
        return availabilities.values();
    }

    @Override
    public Set<Column> keySet() {
        return availabilities.keySet();
    }
}
