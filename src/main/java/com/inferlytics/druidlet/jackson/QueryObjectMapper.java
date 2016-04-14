/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;

/**
 * Deserializes Query Json to be used by the Query helper, and serializes result
 *
 * @author Sriram
 * @since 4/14/2016
 */
public class QueryObjectMapper extends DefaultObjectMapper {
    private static final long serialVersionUID = 1231241241241L;

    public QueryObjectMapper() {
        this((JsonFactory) null);
    }

    public QueryObjectMapper(QueryObjectMapper mapper) {
        super(mapper);
    }

    public QueryObjectMapper(JsonFactory factory) {
        super(factory);
        registerModule(new MissingAggregatorsModule());
    }

    @Override
    public ObjectMapper copy() {
        return new QueryObjectMapper(this);
    }
}
