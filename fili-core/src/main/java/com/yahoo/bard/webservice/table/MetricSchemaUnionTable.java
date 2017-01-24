package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.Availability;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

public class MetricSchemaUnionTable extends BasePhysicalTable implements PhysicalTable {

    public MetricSchemaUnionTable(
            @NotNull String name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNamesCollection,
            Collection<PhysicalTable> dependantTables)
    {
        super(name, timeGrain, columns, logicalToPhysicalColumnNamesCollection);

    }




    @Override
    public Availability getAvailability() {
        return null;
    }





}
