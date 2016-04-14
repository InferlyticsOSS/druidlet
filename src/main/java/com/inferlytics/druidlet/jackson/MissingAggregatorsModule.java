/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.jackson;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.hash.Hashing;
import io.druid.query.aggregation.*;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.histogram.ApproximateHistogramFoldingAggregatorFactory;
import io.druid.query.aggregation.histogram.QuantilePostAggregator;
import io.druid.query.aggregation.histogram.QuantilesPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.aggregation.post.JavaScriptPostAggregator;
import io.druid.segment.serde.ComplexMetrics;

/**
 * Contains the aggregators that are missing from the AggregatorsModule
 *
 * @author Sriram
 * @since 4/14/2016
 */
public class MissingAggregatorsModule extends SimpleModule {
    public MissingAggregatorsModule() {
        super("AggregatorFactories");

        if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
            ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(Hashing.murmur3_128()));
        }

        setMixInAnnotation(AggregatorFactory.class, AggregatorFactoryMixin.class);
        setMixInAnnotation(PostAggregator.class, PostAggregatorMixin.class);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes(value = {
            @JsonSubTypes.Type(name = "count", value = CountAggregatorFactory.class),
            @JsonSubTypes.Type(name = "longSum", value = LongSumAggregatorFactory.class),
            @JsonSubTypes.Type(name = "doubleSum", value = DoubleSumAggregatorFactory.class),
            @JsonSubTypes.Type(name = "doubleMax", value = DoubleMaxAggregatorFactory.class),
            @JsonSubTypes.Type(name = "doubleMin", value = DoubleMinAggregatorFactory.class),
            @JsonSubTypes.Type(name = "longMax", value = LongMaxAggregatorFactory.class),
            @JsonSubTypes.Type(name = "longMin", value = LongMinAggregatorFactory.class),
            @JsonSubTypes.Type(name = "javascript", value = JavaScriptAggregatorFactory.class),
            @JsonSubTypes.Type(name = "histogram", value = HistogramAggregatorFactory.class),
            @JsonSubTypes.Type(name = "hyperUnique", value = HyperUniquesAggregatorFactory.class),
            @JsonSubTypes.Type(name = "cardinality", value = CardinalityAggregatorFactory.class),
            @JsonSubTypes.Type(name = "filtered", value = FilteredAggregatorFactory.class),
            // This is not present in the AggregatorsModule
            @JsonSubTypes.Type(name = "approxHistogramFold", value = ApproximateHistogramFoldingAggregatorFactory.class)
    })
    public static interface AggregatorFactoryMixin {
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes(value = {
            @JsonSubTypes.Type(name = "arithmetic", value = ArithmeticPostAggregator.class),
            @JsonSubTypes.Type(name = "fieldAccess", value = FieldAccessPostAggregator.class),
            @JsonSubTypes.Type(name = "constant", value = ConstantPostAggregator.class),
            @JsonSubTypes.Type(name = "javascript", value = JavaScriptPostAggregator.class),
            @JsonSubTypes.Type(name = "hyperUniqueCardinality", value = HyperUniqueFinalizingPostAggregator.class),
            // These are not present in the AggregatorsModule
            @JsonSubTypes.Type(name = "quantile", value = QuantilePostAggregator.class),
            @JsonSubTypes.Type(name = "quantiles", value = QuantilesPostAggregator.class)
    })
    public static interface PostAggregatorMixin {
    }
}