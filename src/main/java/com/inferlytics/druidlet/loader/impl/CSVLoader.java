/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.loader.impl;

import com.inferlytics.druidlet.loader.Loader;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

/**
 * Reads CSV files with the given column headers, generates InputRow
 * objects with the given dimensions and timestampDimension
 */
public class CSVLoader extends Loader {
    private final Iterator<InputRow> iterator;


    /**
     * @param reader             Reader object pointing to CSV file
     * @param columns            List of all columns in CSV file (including metric and "value" column)
     * @param dimensions         List of dimensions (Excluding metric and value columns)
     * @param timestampDimension Dimension which indicates timestamp field in CSV File.
     */
    public CSVLoader(Reader reader, List<String> columns, List<String> dimensions, String timestampDimension) {
        super(dimensions, timestampDimension);
        this.iterator = new CSVReaderIterator(reader, columns, dimensions, timestampDimension);
    }

    @Override
    public Iterator<InputRow> iterator() {
        return iterator;
    }

    private class CSVReaderIterator implements Iterator<InputRow> {
        private final List<String> columns;
        private final List<String> dimensions;
        private final String timestampDimension;
        private final BufferedReader bufferedReader;
        private String nextLine;

        public CSVReaderIterator(
                final Reader reader,
                final List<String> columns,
                final List<String> dimensions,
                final String timestampDimension) {
            this.columns = columns;
            this.dimensions = dimensions;
            this.timestampDimension = timestampDimension;
            this.bufferedReader = new BufferedReader(reader);
        }

        protected Long getTimestamp(Map<String, Object> map) {
            if (timestampDimension == null) {
                return 1L;
            } else {
                return Long.valueOf((String) map.get(timestampDimension));
            }
        }

        public boolean hasNext() {
            try {
                if (nextLine == null && (nextLine = bufferedReader.readLine()) == null) {
                    close();
                    return false;
                } else {
                    return true;
                }
            } catch (IOException e) {
                e.printStackTrace();
                close();
                return false;
            }
        }

        public InputRow next() {
            if (!hasNext()) {
                return null;
            }
            Map<String, Object> map = parse(nextLine);
            nextLine = null;
            if (map == null) {
                return next();
            }
            return new MapBasedInputRow(getTimestamp(map), dimensions, map);
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public void close() {
            try {
                bufferedReader.close();
            } catch (IOException e) {
            }
        }

        private Map<String, Object> parse(String row) {
            List<String> data = new ArrayList<>();
            // Faster than String.split()
            StringTokenizer stk = new StringTokenizer(row, ",");
            while (stk.hasMoreTokens()) {
                data.add(stk.nextToken());
            }
            Map<String, Object> map = new HashMap<>();
            if (data.size() != columns.size()) {
                return null;
            }
            for (int i = 0; i < columns.size(); i++) {
                if (!data.get(i).equals("null") && !data.get(i).isEmpty()) {
                    map.put(columns.get(i), data.get(i));
                }
            }
            return map;
        }
    }
}
