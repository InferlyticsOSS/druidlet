/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.service;

import com.inferlytics.druidlet.core.DruidIndices;
import com.inferlytics.druidlet.core.QueryExecutor;
import com.inferlytics.druidlet.util.Utils;
import com.metamx.common.guava.Sequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handles queries from DruidResource and returns the response
 *
 * @author Sriram
 * @since 4/14/2016
 */
public class DruidService {
    private static final Logger LOG = LoggerFactory.getLogger(DruidService.class);
    private static final DruidIndices INDICES = DruidIndices.getInstance();

    public static Sequence handleQuery(String indexKey, String queryJson) throws IOException {
        LOG.trace("Got query: " + queryJson);
        return QueryExecutor.run(Utils.getQuery(queryJson), INDICES.get(indexKey));
    }
}
