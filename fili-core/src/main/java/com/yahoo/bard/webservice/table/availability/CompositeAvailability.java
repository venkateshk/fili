// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An availability which guarantees immutability on its contents.
 */
public class CompositeAvailability implements Availability {

    private final Set<Availability> dependantAvailabilities;

    private Function<List<List<Interval>>, List<Interval>>
    //private final Map<Column, Supplier<List<Interval>>> foo;

    /**
     * Constructor.
     *
     * @param physicalTables A map of columns to lists of available intervals
     */
    public CompositeAvailability(Set<PhysicalTable> physicalTables) {
        dependantAvailabilities = physicalTables.stream()
                .map(PhysicalTable::getAvailability).collect(Collectors.toSet());
    }


    Function<List<List<Interval>>, Stream<List<Interval>>>

//List<Interval>

    @Override
    public List<Interval> get(Column column) {
        if (column instanceof  DimensionColumn) {
            Optional<SimplifiedIntervalList> intervals = dependantAvailabilities.stream()
                    .map(availability -> availability.get(column))
                    .map(SimplifiedIntervalList::new)
                    .reduce(SimplifiedIntervalList::union);
            return intervals.orElse(new SimplifiedIntervalList());
        }
        List<List<Interval>> metricIntervalLists = dependantAvailabilities.stream()
                .map(availability -> availability.get(column))
                .collect(Collectors.toList());

        return validator.apply(metricIntervalLists);
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
