/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.helper;

import io.druid.granularity.QueryGranularity;
import io.druid.query.Query;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.spec.QuerySegmentSpecs;
import io.druid.query.topn.TopNQueryBuilder;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Creates Query objects to be used for testing
 *
 * @author Sriram
 * @since 4/14/2016
 */
public class QueryCreationHelper {
    public static Query getGroupByQuery() {
        List<DimFilter> filters = new ArrayList<DimFilter>();
        filters.add(DimFilters.dimEquals("report", "URLTransaction"));
        filters.add(DimFilters.dimEquals("pool", "r1cart"));
        filters.add(DimFilters.dimEquals("metric", "Duration"));
        return GroupByQuery.builder()
                .setDataSource("test")
                .setQuerySegmentSpec(QuerySegmentSpecs.create(new Interval(0, new DateTime().getMillis())))
                .setGranularity(QueryGranularity.NONE)
                .addDimension("URL")
                .addAggregator(new LongSumAggregatorFactory("agg_count", "agg_count"))
                .addAggregator(new DoubleMaxAggregatorFactory("agg_max", "agg_max"))
                .addAggregator(new DoubleMinAggregatorFactory("agg_min", "agg_min"))
                .addAggregator(new DoubleSumAggregatorFactory("agg_sum", "agg_sum"))
                .setDimFilter(DimFilters.and(filters))
                .build();
    }

    public static Query getTopNQuery() {
        List<DimFilter> filters = new ArrayList<DimFilter>();
        filters.add(DimFilters.dimEquals("report", "URLTransaction"));
        filters.add(DimFilters.dimEquals("pool", "r1cart"));
        filters.add(DimFilters.dimEquals("metric", "Duration"));
        return new TopNQueryBuilder()
                .threshold(5)
                .metric("agg_count")
                .dataSource("test")
                .intervals(QuerySegmentSpecs.create(new Interval(0, new DateTime().getMillis())))
                .granularity(QueryGranularity.NONE)
                .dimension("colo")
                .aggregators(
                        Arrays.asList(
                                new LongSumAggregatorFactory("agg_count", "agg_count"),
                                new DoubleMaxAggregatorFactory("agg_max", "agg_max"),
                                new DoubleMinAggregatorFactory("agg_min", "agg_min"),
                                new DoubleSumAggregatorFactory("agg_sum", "agg_sum")))
                .filters(DimFilters.and(filters)).build();
    }
}
