/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ListenableFuture;
import com.inferlytics.druidlet.util.Utils;
import com.metamx.common.guava.Sequence;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.collections.StupidPool;
import io.druid.query.*;
import io.druid.query.groupby.*;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import io.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.SearchQueryRunnerFactory;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryEngine;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.select.SelectQueryRunnerFactory;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Executes Queries on Indexes defined in the QueryRunnerFactoryConglomerate
 *
 * @author Sriram
 * @since 4/14/2016
 */
public class QueryExecutor {
    private static final int MAX_TOTAL_BUFFER_SIZE;
    private static final ServiceEmitter NOOP_SERVICE_EMITTER;
    private static final QueryWatcher NOOP_QUERYWATCHER;
    private static final QueryRunnerFactoryConglomerate CONGLOMERATE;


    static {
        MAX_TOTAL_BUFFER_SIZE = 1024 * 1024 * 1024;
        NOOP_SERVICE_EMITTER = new ServiceEmitter(null, null, null) {
            @Override
            public void emit(Event event) {
            }
        };
        NOOP_QUERYWATCHER = new QueryWatcher() {
            @SuppressWarnings("rawtypes")
            public void registerQuery(Query query, ListenableFuture future) {
            }
        };
        HashMap<Class<? extends Query>, QueryRunnerFactory> map = new HashMap<>();

        // Register all query runner factories.
        map.put(GroupByQuery.class, getGroupByQueryRunnerFactory());
        map.put(TopNQuery.class, getTopNQueryRunnerFactory());
        map.put(SearchQuery.class, getSearchQueryRunnerFactory());
        map.put(SelectQuery.class, getSelectQueryRunnerFactory());
        map.put(SegmentMetadataQuery.class, getSegmentMetadataQueryRunnerFactory());
        map.put(TimeseriesQuery.class, getTimeseriesQueryRunnerFactory());
        map.put(TimeBoundaryQuery.class, getTimeBoundaryQueryRunnerFactory());

        CONGLOMERATE = new DefaultQueryRunnerFactoryConglomerate(map);
    }

    private static class ByteBufferSupplier implements Supplier<ByteBuffer> {
        int capacity;

        public ByteBufferSupplier(int capacity) {
            this.capacity = capacity;
        }

        public ByteBuffer get() {
            return ByteBuffer.allocate(capacity);
        }
    }

    private static StupidPool<ByteBuffer> getBufferPool() {
        return new StupidPool<>(new ByteBufferSupplier(MAX_TOTAL_BUFFER_SIZE / 2));
    }


    private static IntervalChunkingQueryRunnerDecorator noopIntervalChunkingQueryRunnerDecorator() {
        return new IntervalChunkingQueryRunnerDecorator(null, null, null) {
            @Override
            public <T> QueryRunner<T> decorate(final QueryRunner<T> delegate,
                                               QueryToolChest<T, ? extends Query<T>> toolChest) {
                return new QueryRunner<T>() {
                    public Sequence<T> run(Query<T> query, Map<String, Object> responseContext) {
                        return delegate.run(query, responseContext);
                    }
                };
            }
        };
    }

	/*
     * All subclasses of Query with default configuration.
	 */

    private static TimeseriesQueryRunnerFactory getTimeseriesQueryRunnerFactory() {
        TimeseriesQueryQueryToolChest toolChest =
                new TimeseriesQueryQueryToolChest(noopIntervalChunkingQueryRunnerDecorator());
        TimeseriesQueryEngine engine = new TimeseriesQueryEngine();
        return new TimeseriesQueryRunnerFactory(toolChest, engine, NOOP_QUERYWATCHER);
    }

    private static TimeBoundaryQueryRunnerFactory getTimeBoundaryQueryRunnerFactory() {
        return new TimeBoundaryQueryRunnerFactory(NOOP_QUERYWATCHER);
    }

    private static SegmentMetadataQueryRunnerFactory getSegmentMetadataQueryRunnerFactory() {
        SegmentMetadataQueryConfig config = new SegmentMetadataQueryConfig();
        SegmentMetadataQueryQueryToolChest toolChest = new SegmentMetadataQueryQueryToolChest(config);
        return new SegmentMetadataQueryRunnerFactory(toolChest, NOOP_QUERYWATCHER);
    }

    private static SelectQueryRunnerFactory getSelectQueryRunnerFactory() {
        SelectQueryQueryToolChest toolChest =
                new SelectQueryQueryToolChest(new ObjectMapper(),
                        noopIntervalChunkingQueryRunnerDecorator());
        SelectQueryEngine engine = new SelectQueryEngine();
        return new SelectQueryRunnerFactory(toolChest, engine, NOOP_QUERYWATCHER);
    }

    private static SearchQueryRunnerFactory getSearchQueryRunnerFactory() {
        SearchQueryQueryToolChest toolChest =
                new SearchQueryQueryToolChest(new SearchQueryConfig(),
                        noopIntervalChunkingQueryRunnerDecorator());
        return new SearchQueryRunnerFactory(toolChest, NOOP_QUERYWATCHER);
    }

    private static TopNQueryRunnerFactory getTopNQueryRunnerFactory() {
        TopNQueryQueryToolChest toolChest =
                new TopNQueryQueryToolChest(new TopNQueryConfig(),
                        noopIntervalChunkingQueryRunnerDecorator());
        return new TopNQueryRunnerFactory(getBufferPool(), toolChest, NOOP_QUERYWATCHER);
    }

    private static GroupByQueryRunnerFactory getGroupByQueryRunnerFactory() {
        GroupByQueryConfig config = new GroupByQueryConfig();
        config.setMaxIntermediateRows(10000);

        Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
        GroupByQueryEngine engine = new GroupByQueryEngine(configSupplier, getBufferPool());

        return new GroupByQueryRunnerFactory(engine, NOOP_QUERYWATCHER, configSupplier,
                new GroupByQueryQueryToolChest(configSupplier, Utils.JSON_MAPPER, engine, getBufferPool(),
                        noopIntervalChunkingQueryRunnerDecorator()), getBufferPool());
    }

    private static QueryRunnerFactory findFactory(Query query) {
        return CONGLOMERATE.findFactory(query);
    }

    /**
     * Executes a Query by identifying the appropriate QueryRunner
     *
     * @param query Query to execute
     * @param index Index to execute query on
     * @return Result of the query
     */
    @SuppressWarnings("unchecked")
    public static Sequence run(Query query, QueryableIndex index) {
        return findFactory(query).createRunner(new QueryableIndexSegment("", index)).run(query, null);
    }

}
