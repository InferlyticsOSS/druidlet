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
 * This abstract class is interface for loading data of various formats. Possible implementation can be
 * CSV, XML, JSON etc. Implementation class needs to provide iterator implementation. 
 *
 */
public abstract class Loader implements Iterable<InputRow> {
	  protected List<String> columns;
	  protected List<String> dimensions;
	  protected String timestampDimension;
	  
	  public Loader(List<String> cols, List<String> dims, String ts) {
		  this.columns = cols;
		  this.dimensions = dims;
		  this.timestampDimension = ts;
	  }
}
