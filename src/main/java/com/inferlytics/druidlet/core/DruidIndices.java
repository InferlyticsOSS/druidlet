/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.core;

import com.inferlytics.druidlet.core.compatibility.druid.SlowIndexMerger;
import com.inferlytics.druidlet.util.OSCheck;
import io.druid.data.input.InputRow;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds and caches QueryableIndex objects
 *
 * @author Sriram
 * @since 4/14/2016
 */
public class DruidIndices {
    private static final DruidIndices INSTANCE;

    static {
        INSTANCE = new DruidIndices();
    }

    public static DruidIndices getInstance() {
        return INSTANCE;
    }

    private Map<String, QueryableIndex> indexMap;

    private DruidIndices() {
        this.indexMap = new HashMap<>();
    }

    /**
     * Builds and caches a QueryableIndex from an Iterable by building, persisting and reloading an IncrementalIndex
     *
     * @param dataSource  Key to store the index under
     * @param loader      Iterable&lt;InputRow&gt; object to read data rows from
     * @param indexSchema Schema of the index
     * @return QueryableIndex that was cached
     * @throws IOException
     */
    public QueryableIndex cache(String dataSource, Iterable<InputRow> loader, IncrementalIndexSchema indexSchema)
            throws IOException {
        IncrementalIndex<?> incIndex = new OnheapIncrementalIndex(indexSchema, true, Integer.MAX_VALUE);

        for (InputRow row : loader) {
            incIndex.add(row);
        }
        String tmpDir = System.getProperty("druid.segment.dir");
        if (tmpDir == null) {
            tmpDir = System.getProperty("java.io.tmpdir") + File.separator + "druid-tmp-index-";
        }
        File tmpIndexDir = new File(tmpDir + loader.hashCode());
        IndexIO indexIO = new IndexIO(new DefaultObjectMapper(), new ColumnConfig() {
            @Override
            public int columnCacheSizeBytes() {
                return 0;
            }
        });
        if (OSCheck.isWindows()) {
            new SlowIndexMerger(new DefaultObjectMapper(), indexIO).persist(incIndex, tmpIndexDir, new IndexSpec());
        } else {
            new IndexMerger(new DefaultObjectMapper(), indexIO).persist(incIndex, tmpIndexDir, new IndexSpec());
        }
        this.indexMap.put(dataSource, indexIO.loadIndex(tmpIndexDir));
        return this.indexMap.get(dataSource);
    }

    /**
     * Returns a cached index
     *
     * @param dataSource Key of the index
     * @return QueryableIndex held under this dataSource
     */
    public QueryableIndex get(String dataSource) {
        return this.indexMap.get(dataSource);
    }

    /**
     * Caches a QueryableIndex at the given key
     *
     * @param indexKey Key to store the index under
     * @param index    Index to cache
     */
    public void cache(String indexKey, QueryableIndex index) {
        this.indexMap.put(indexKey, index);
    }

    /**
     * Removes an index by key
     *
     * @param indexKey Key to un-cache
     */
    public void remove(String indexKey) {
        this.indexMap.remove(indexKey);
    }

    /**
     * Checks if an index exists by key
     *
     * @param indexKey Key to check
     */
    public boolean has(String indexKey) {
        return this.indexMap.containsKey(indexKey);
    }
}
