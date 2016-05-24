/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.loader;

import com.inferlytics.druidlet.loader.impl.CSVLoader;
import com.inferlytics.druidlet.loader.impl.JsonLoader;
import io.druid.data.input.InputRow;

import java.io.Reader;
import java.util.List;


/**
 * Abstract class which acts as an interface for loading files onto a QueryableIndex
 * Must be implemented to support CSV, TSV, JSON, XML, etc.
 */
public abstract class Loader implements Iterable<InputRow>
{
    protected List<String> dimensions;
    protected String timestampDimension;


    public Loader( List<String> dims, String ts )
    {
        this.dimensions = dims;
        this.timestampDimension = ts;
    }


    /**
     * CSVLoader implementation of the Loader
     *
     * @param reader             A Reader opened to the CSV file
     * @param columns            List of columns in the CSV
     * @param dimensions         List of dimensions to index
     * @param timestampDimension Timestamp dimension
     * @return A new CSVLoader to the CSV file specified by the reader
     */
    public static Loader csv( Reader reader, List<String> columns, List<String> dimensions, String timestampDimension )
    {
        return new CSVLoader( reader, columns, dimensions, timestampDimension );
    }


    public static Loader json( Reader reader, List<String> dimensions, String timestampDimension )
    {
        return new JsonLoader(reader, dimensions, timestampDimension);
    }
}
