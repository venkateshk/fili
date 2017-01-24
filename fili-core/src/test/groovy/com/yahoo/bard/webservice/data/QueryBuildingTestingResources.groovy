// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.COLOR
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SHAPE
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SIZE
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.dimension.LookupDimensionConfig
import com.yahoo.bard.webservice.data.config.dimension.TestLookupDimensions
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricColumn
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.time.TimeGrain
import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsFunction
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConcretePhysicalTable
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTimeZone
import org.joda.time.Interval

public class QueryBuildingTestingResources {

    public DimensionColumn d1, d2, d3, d4, d5, d6, d7, d8, d9, d10

    public LogicalMetricColumn m1, m2, m3, m4, m5, m6

    public Interval interval1, interval2, interval3

    public ConcretePhysicalTable t1h, t1d, t1hShort, t2h, t3d, t4h1, t4h2, t4d1, t4d2, t5h, emptyFirst, partialSecond, wholeThird, emptyLast, tna1236d, tna1237d, tna167d, tna267d, volatileHourTable, volatileDayTable

    public VolatileIntervalsService volatileIntervalsService

    public TableGroup tg1h, tg1d, tg1Short, tg2h, tg3d, tg4h, tg5h, tg6h, tg1All, tgna

    public LogicalTable lt12, lt13, lt14, lt1All, ltna

    public LogicalTableDictionary logicalDictionary

    public TableIdentifier ti2h, ti2d, ti3d, ti4d, ti1All, tina

    public TemplateDruidQuery simpleTemplateQuery, simpleNestedTemplateQuery, complexTemplateQuery, simpleTemplateWithGrainQuery, complexTemplateWithInnerGrainQuery, complexTemplateWithDoubleGrainQuery

    public DimensionDictionary dimensionDictionary

    public MetricDictionary metricDictionary

    public QueryBuildingTestingResources() {

        DateTimeZone.setDefault(UTC)
        def ages = ["1": "0-10", "2": "11-14", "3": "14-29", "4": "30-40", "5": "41-59", "6": "60+"]

        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        d1 = new DimensionColumn( new KeyValueStoreDimension(
                "dim1",
                "dim1",
                dimensionFields,
                MapStoreManager.getInstance("dim1"),
                ScanSearchProviderManager.getInstance("dim1")
        ))

        d2 = new DimensionColumn(new KeyValueStoreDimension(
                "dim2",
                "dim2",
                dimensionFields,
                MapStoreManager.getInstance("dim2"),
                ScanSearchProviderManager.getInstance("dim2")
        ))

        d2.getDimension().addAllDimensionRows(ages.collect { BardDimensionField.makeDimensionRow(d2.getDimension(), it.key, it.value) } as Set)

        d3 = new DimensionColumn(new KeyValueStoreDimension(
                "ageBracket",
                "age_bracket",
                dimensionFields,
                MapStoreManager.getInstance("ageBracket"),
                ScanSearchProviderManager.getInstance("ageBracket")
        ))

        d3.getDimension().addAllDimensionRows(ages.collect { BardDimensionField.makeDimensionRow(d3.getDimension(), it.key, it.value) } as Set)

        d4 = new DimensionColumn(new KeyValueStoreDimension(
                "dim4",
                "dim4",
                dimensionFields,
                MapStoreManager.getInstance("dim4"),
                ScanSearchProviderManager.getInstance("dim4")
        ))

        d5 = new DimensionColumn(new KeyValueStoreDimension(
                "dim5",
                "dim5",
                dimensionFields,
                MapStoreManager.getInstance("dim5"),
                ScanSearchProviderManager.getInstance("dim5")
        ))

        d6 = new DimensionColumn(new KeyValueStoreDimension(
                "dim6",
                "dim6",
                dimensionFields,
                MapStoreManager.getInstance("dim6"),
                ScanSearchProviderManager.getInstance("dim6"),
                false
        ))

        d7 = new DimensionColumn(new KeyValueStoreDimension(
                "dim7",
                "dim_7",
                dimensionFields,
                MapStoreManager.getInstance("dim7"),
                ScanSearchProviderManager.getInstance("dim7"),
                false
        ))

        LinkedHashSet<DimensionConfig> dimConfig = new TestLookupDimensions().getDimensionConfigurationsByApiName(SIZE, SHAPE, COLOR)

        d8 = new DimensionColumn(new LookupDimension((LookupDimensionConfig) dimConfig.getAt(0)))
        d9 = new DimensionColumn(new LookupDimension((LookupDimensionConfig) dimConfig.getAt(1)))
        d10 = new DimensionColumn(new LookupDimension((LookupDimensionConfig) dimConfig.getAt(2)))

        dimensionDictionary = new DimensionDictionary()
        dimensionDictionary.addAll([d1, d2, d3, d4, d5, d6, d7, d8, d9, d10].collect { it.getDimension() })

        m1 = new LogicalMetricColumn(new LogicalMetric(null, null, "metric1"))
        m2 = new LogicalMetricColumn(new LogicalMetric(null, null, "metric2"))
        m3 = new LogicalMetricColumn(new LogicalMetric(null, null, "metric3"))
        m4 = new LogicalMetricColumn(new LogicalMetric(null, null, "metric4"))
        m5 = new LogicalMetricColumn(new LogicalMetric(null, null, "metric5"))
        m6 = new LogicalMetricColumn(new LogicalMetric(null, null, "metric6"))

        metricDictionary = new MetricDictionary()
        [m1, m2, m3, m4, m5, m6].each {
            metricDictionary.add(it.getLogicalMetric())
        }

        interval1 = new Interval("2014-06-23/2014-07-14")
        interval2 = new Interval("2014-07-07/2014-07-21")
        interval3 = new Interval("2014-07-01/2014-08-01")

        TimeGrain utcHour = HOUR.buildZonedTimeGrain(UTC)
        TimeGrain utcDay = DAY.buildZonedTimeGrain(UTC)

        volatileHourTable = new ConcretePhysicalTable("hour", HOUR.buildZonedTimeGrain(UTC), [d1, m1].toSet(), [:])
        volatileDayTable = new ConcretePhysicalTable("day", DAY.buildZonedTimeGrain(UTC), [d1, m1].toSet(), [:])

        t1h = new ConcretePhysicalTable("table1h", utcHour, [d1, d2, d3, m1, m2, m3].toSet(), ["ageBracket":"age_bracket"])
        t1d = new ConcretePhysicalTable("table1d", utcDay, [d1, d2, d3, m2, m3].toSet(), ["ageBracket":"age_bracket"])
        t1hShort = new ConcretePhysicalTable("table1Short", utcHour, [d1, d2, m1, m2, m3].toSet(), [:])

        t2h = new ConcretePhysicalTable("table2", utcHour, [d1, d2, d4, m1, m4, m5].toSet(), [:])
        t3d = new ConcretePhysicalTable("table3", utcDay, [d1, d2, d5, m6].toSet(), [:])

        tna1236d = new ConcretePhysicalTable("tableNA1236", utcDay, [d1, d2, d3, d6].toSet(),["ageBracket":"age_bracket"])
        tna1237d = new ConcretePhysicalTable("tableNA1237", utcDay, [d1, d2, d3, d7].toSet(), ["ageBracket":"age_bracket"])
        tna167d = new ConcretePhysicalTable("tableNA167", utcDay, [d1, d6, d7].toSet(), ["ageBracket":"age_bracket", "dim7":"dim_7"])
        tna267d = new ConcretePhysicalTable("tableNA267", utcDay, [d2, d6, d7].toSet(), ["dim7":"dim_7"])

        t4h1 = new ConcretePhysicalTable("table4h1", utcHour, [d1, d2, m1, m2, m3].toSet(), [:])
        t4h2 = new ConcretePhysicalTable("table4h2", utcHour, [d1, d2, m1, m2, m3].toSet(), [:])
        t4d1 = new ConcretePhysicalTable("table4d1", utcDay, [d1, d2, m1, m2, m3].toSet(), [:])
        t4d2 = new ConcretePhysicalTable("table4d2", utcDay, [d1, d2, m1, m2, m3].toSet(), [:])

        t5h = new ConcretePhysicalTable("table5d", utcHour, [d8, d9, d10, m1].toSet(), [:])

        Map<Column, List<Interval>> availabilityMap1 = [:]
        Map<Column, List<Interval>> availabilityMap2 = [:]

        [d1, d2, m1, m2, m3].each {
            availabilityMap1.put(it, [interval1])
            availabilityMap2.put(it, [interval2])
        }

        t4h1.setAvailability(availabilityMap1)
        t4d1.setAvailability(availabilityMap1)

        t4h2.setAvailability(availabilityMap2)
        t4d2.setAvailability(availabilityMap2)

        tg1h = new TableGroup([t1h, t1d, t1hShort] as LinkedHashSet, [])
        tg1d = new TableGroup([t1d] as LinkedHashSet, [])
        tg1Short = new TableGroup([t1hShort] as LinkedHashSet, [])
        tg2h = new TableGroup([t2h] as LinkedHashSet, [])
        tg3d = new TableGroup([t3d] as LinkedHashSet, [])
        tg4h = new TableGroup([t1h, t2h] as LinkedHashSet, [])
        tg5h = new TableGroup([t2h, t1h] as LinkedHashSet, [])
        tg6h = new TableGroup([t5h] as LinkedHashSet, [])
        tgna = new TableGroup([tna1236d, tna1237d, tna167d, tna267d] as LinkedHashSet, [])

        lt12 = new LogicalTable("base12", HOUR, tg1h, metricDictionary)
        lt13 = new LogicalTable("base13", DAY, tg1d, metricDictionary)
        lt14 = new LogicalTable("base14", HOUR, tg6h, metricDictionary)
        lt1All = new LogicalTable("baseAll", AllGranularity.INSTANCE, tg1All, metricDictionary)
        ltna = new LogicalTable("baseNA", AllGranularity.INSTANCE, tgna, metricDictionary)

        ti2h = new TableIdentifier("base12", HOUR)
        ti2d = new TableIdentifier("base12", DAY)
        ti3d = new TableIdentifier("base13", DAY)
        ti4d = new TableIdentifier("base14", HOUR)
        tina = new TableIdentifier("baseNA", DAY)

        Map baseMap = [
                (ti2h): lt12,
                (ti2d): lt12,
                (ti3d): lt13,
                (ti4d): lt14,
                (ti1All): lt1All,
                (tina): ltna
        ]

        logicalDictionary = new LogicalTableDictionary()
        logicalDictionary.putAll(baseMap)

        simpleTemplateQuery = new TemplateDruidQuery(new LinkedHashSet(), new LinkedHashSet(), null, null)
        simpleNestedTemplateQuery = simpleTemplateQuery.nest()
        complexTemplateQuery = new TemplateDruidQuery(
                new LinkedHashSet(),
                new LinkedHashSet(),
                simpleTemplateQuery,
                null
        )

        simpleTemplateWithGrainQuery = new TemplateDruidQuery(new LinkedHashSet(), new LinkedHashSet(), DAY)
        complexTemplateWithInnerGrainQuery = new TemplateDruidQuery(
                new LinkedHashSet(),
                new LinkedHashSet(),
                simpleTemplateWithGrainQuery,
                null
        )
        complexTemplateWithDoubleGrainQuery = new TemplateDruidQuery(
                new LinkedHashSet(),
                new LinkedHashSet(),
                simpleTemplateWithGrainQuery,
                WEEK
        )
        /* Example:
        List<Column> columns = []
        ConcretePhysicalTable table1 = ConcretePhysicalTable("name", DAY, columns, [:] )

        Map<Column, List<Interval>> availableMap = columns.collectEntries { [(it): [interval2] ]}
        table1.setAvailability(new SimpleAvailability(availableMap))
         */
        setupPartialData()
    }

    def setupPartialData() {
        // In the event of partiality on all data, the coarsest table will be selected and the leftmost of the
        // coarsest tables should be selected
        emptyFirst = new ConcretePhysicalTable("emptyFirst", MONTH.buildZonedTimeGrain(UTC), [d1, m1, m2, m3].toSet(), [:])
        emptyLast = new ConcretePhysicalTable("emptyLast", MONTH.buildZonedTimeGrain(UTC), [d1, m1, m2, m3].toSet(), [:])
        partialSecond = new ConcretePhysicalTable("partialSecond", MONTH.buildZonedTimeGrain(UTC), [d1, m1, m2, m3].toSet(), [:])
        wholeThird = new ConcretePhysicalTable("wholeThird", MONTH.buildZonedTimeGrain(UTC), [d1, m1, m2, m3].toSet(), [:])

        Map<Column, List<Interval>> availabilityMap1 = [:]
        Map<Column, List<Interval>> availabilityMap2 = [:]
        Map<Column, List<Interval>> availabilityMap3 = [:]

        [d1, d2, m1, m2, m3].each {
            availabilityMap1.put(it, [new Interval("2015/2015")])
            availabilityMap2.put(it, [new Interval("2015/2016")])
            availabilityMap3.put(it, [new Interval("2011/2016")])
        }

        emptyFirst.setAvailability(availabilityMap1)
        emptyLast.setAvailability(availabilityMap1)
        partialSecond.setAvailability(availabilityMap2)
        wholeThird.setAvailability(availabilityMap3)

        tg1All = new TableGroup([emptyFirst, partialSecond, wholeThird, emptyLast] as LinkedHashSet, [])
        ti1All = new TableIdentifier("base1All", AllGranularity.INSTANCE)
    }

    /**
     * Given a collection of triples, each of which contains a physical table, an interval representing the table's
     * availability, and an interval representing the table's volatility, adds the availability information to the
     * table, and initializes the volatileIntervalsService field to map to the correct volatility information.
     *
     * @param physicalTableAvailabilityVolatilityTriples  The collection of triples containing the physical tables,
     * availability, and volatility information
     */
    def setupVolatileTables(Collection<Collection> physicalTableAvailabilityVolatilityTriples) {
        physicalTableAvailabilityVolatilityTriples.each { ConcretePhysicalTable table, Interval availability, _ ->
            table.setAvailability([d1, m1].collectEntries() {
                [it: [availability]]
            })
        }

        volatileIntervalsService = new DefaultingVolatileIntervalsService(
                {[] as SimplifiedIntervalList} as VolatileIntervalsFunction,
                physicalTableAvailabilityVolatilityTriples
                        .collectEntries { PhysicalTable table, _, Interval volatility ->
                                [(table): ({ new SimplifiedIntervalList([volatility]) } as VolatileIntervalsFunction)]
                        }
        )
    }
}
