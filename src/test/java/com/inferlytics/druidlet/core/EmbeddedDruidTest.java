/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.core;

import com.google.common.collect.Lists;
import com.inferlytics.druidlet.helper.IndexCreationHelper;
import com.inferlytics.druidlet.helper.QueryCreationHelper;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.segment.QueryableIndex;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;

public class EmbeddedDruidTest {

    @Test
    public void groupByQuery() throws IOException {
        QueryableIndex index = IndexCreationHelper.getIndex("groupByTest");
        Query query = QueryCreationHelper.getGroupByQuery();

        @SuppressWarnings("unchecked")
        Sequence<Row> sequence = QueryExecutor.run(query, index);
        ArrayList<Row> results = Sequences.toList(sequence, Lists.<Row>newArrayList());
        Assert.assertEquals(results.size(), 2);

        if (results.get(0).getDimension("URL").get(0).equals("abc")) {
            Assert.assertEquals(results.get(0).getLongMetric("agg_sum"), 247);
            Assert.assertEquals(results.get(0).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(0).getLongMetric("agg_max"), 124);
            Assert.assertEquals(results.get(0).getLongMetric("agg_count"), 12);
            Assert.assertEquals(results.get(1).getLongMetric("agg_sum"), 123);
            Assert.assertEquals(results.get(1).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(1).getLongMetric("agg_max"), 123);
            Assert.assertEquals(results.get(1).getLongMetric("agg_count"), 3);

        } else {
            Assert.assertEquals(results.get(0).getLongMetric("agg_sum"), 123);
            Assert.assertEquals(results.get(0).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(0).getLongMetric("agg_max"), 123);
            Assert.assertEquals(results.get(0).getLongMetric("agg_count"), 3);
            Assert.assertEquals(results.get(1).getLongMetric("agg_sum"), 247);
            Assert.assertEquals(results.get(1).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(1).getLongMetric("agg_max"), 124);
            Assert.assertEquals(results.get(1).getLongMetric("agg_count"), 12);
        }
    }

    @Test
    public void topNQuery() throws IOException {
        QueryableIndex index = IndexCreationHelper.getIndex("topNTest");
        Query query = QueryCreationHelper.getTopNQuery();
        @SuppressWarnings("unchecked")
        Sequence<Result> sequence = QueryExecutor.run(query, index);
        ArrayList<Result> results = Sequences.toList(sequence, Lists.<Result>newArrayList());
        Assert.assertEquals(results.size(), 1);
    }

}
