/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.loader;

import io.druid.data.input.InputRow;

import java.util.List;

/**
 * Abstract class which acts as an interface for loading files onto a QueryableIndex
 * Must be implemented to support CSV, TSV, JSON, XML, etc.
 */
public abstract class Loader implements Iterable<InputRow> {
    protected List<String> dimensions;
    protected String timestampDimension;

    public Loader(List<String> dims, String ts) {
        this.dimensions = dims;
        this.timestampDimension = ts;
    }
}
