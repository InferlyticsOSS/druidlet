/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inferlytics.druidlet.jackson.QueryObjectMapper;
import io.druid.query.Query;

import java.io.IOException;
import java.io.InputStream;

/**
 * Utility methods and objects
 *
 * @author Sriram
 * @since 4/14/2016
 */
public class Utils {
    // Mapper to be used everywhere
    public static final ObjectMapper JSON_MAPPER = new QueryObjectMapper();


    /**
     * Converts the given query JSON into a Query object
     *
     * @param queryInputStream query JSON stream
     * @return Query object containing the query
     * @throws JsonParseException   Thrown if JSON parsing failed
     * @throws JsonMappingException Thrown if there was a problem mapping the JSON
     * @throws IOException          Thrown by the readValue method
     */
    public static Query getQuery(InputStream queryInputStream) throws JsonParseException,
            JsonMappingException, IOException {
        return JSON_MAPPER.readValue(queryInputStream, Query.class);
    }

    /**
     * Converts the given query JSON into a Query object
     *
     * @param queryJson query JSON String
     * @return Query object containing the query
     * @throws JsonParseException   Thrown if JSON parsing failed
     * @throws JsonMappingException Thrown if there was a problem mapping the JSON
     * @throws IOException          Thrown by the readValue method
     */
    public static Query getQuery(String queryJson) throws JsonParseException,
            JsonMappingException, IOException {
        return JSON_MAPPER.readValue(queryJson, Query.class);
    }
}
