/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.helper;

import com.inferlytics.druidlet.core.DruidIndices;
import com.inferlytics.druidlet.loader.Loader;
import com.inferlytics.druidlet.loader.impl.CSVLoader;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.*;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds index and returns cached index for tests
 *
 * @author Sriram
 * @since 4/14/2016
 */
public class IndexCreationHelper {
    private static final DruidIndices indices = DruidIndices.getInstance();

    public static QueryableIndex getIndex(String indexKey) throws IOException {
        if (indices.has(indexKey)) {
            return indices.get(indexKey);
        }
        //  Create druid segments from raw data
        Reader reader = new FileReader(new File("./src/test/resources/report.csv"));

        List<String> columns = Arrays.asList("colo", "pool", "report", "URL", "TS", "metric", "value", "count", "min", "max", "sum");
        List<String> metrics = Arrays.asList("value", "count", "min", "max", "sum");
        List<String> dimensions = new ArrayList<>(columns);
        dimensions.removeAll(metrics);
        Loader loader = new CSVLoader(reader, columns, dimensions, "TS");

        DimensionsSpec dimensionsSpec = new DimensionsSpec(dimensions, null, null);
        AggregatorFactory[] metricsAgg = new AggregatorFactory[]{
                new LongSumAggregatorFactory("agg_count", "count"),
                new DoubleMaxAggregatorFactory("agg_max", "max"),
                new DoubleMinAggregatorFactory("agg_min", "min"),
                new DoubleSumAggregatorFactory("agg_sum", "sum")
        };
        IncrementalIndexSchema indexSchema = new IncrementalIndexSchema(0, QueryGranularity.ALL, dimensionsSpec, metricsAgg);
        indices.cache(indexKey, loader, indexSchema);
        return indices.get(indexKey);
    }
}
