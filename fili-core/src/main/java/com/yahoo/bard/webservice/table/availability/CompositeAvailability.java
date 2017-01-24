// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An availability which guarantees immutability on its contents.
 */
public class CompositeAvailability implements Availability {

    private final Set<Availability> dependantAvailabilities;

    private final Function<List<List<Interval>>, List<Interval>> metricIntervalReducer;

    /**
     * Constructor.
     *
     * @param physicalTables A map of columns to lists of available intervals
     */
    public CompositeAvailability(Set<PhysicalTable> physicalTables) {
        dependantAvailabilities = physicalTables.stream()
                .map(PhysicalTable::getAvailability).collect(Collectors.toSet());

        metricIntervalReducer = (metricAvailabilitiesList) -> {
            if (metricAvailabilitiesList.size() > 1) {
                throw new RuntimeException("Good exception message for invalid availability");
            }
            return metricAvailabilitiesList.size() == 0 ?
                    new SimplifiedIntervalList() :
                    metricAvailabilitiesList.get(0);
        };
    }

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

        return metricIntervalReducer.apply(metricIntervalLists);
    }

    @Override
    public Set<List<Interval>> values() {
        return keySet().stream()
                .map(this::get)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Column> keySet() {
        return dependantAvailabilities.stream()
                .map(Availability::keySet)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }
}
