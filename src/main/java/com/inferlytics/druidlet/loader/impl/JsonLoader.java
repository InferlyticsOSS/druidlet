package com.inferlytics.druidlet.loader.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inferlytics.druidlet.loader.Loader;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Reads JSON files with the given column headers, generates InputRow
 * objects with the given dimensions and timestampDimension
 * Created by Samarth Bhargav on 5/11/16.
 */
public class JsonLoader extends Loader
{
    private static final TypeReference<HashMap<String, Object>> TYPE_REFERENCE = new TypeReference<HashMap<String, Object>>()
    {
    };

    private final Reader reader;


    /**
     * Create a new JSON reader
     * @param reader the reader to read from
     * @param dimensions the list of dimensions
     * @param timestampDimension the time stamp dimensions
     */
    public JsonLoader( Reader reader, List<String> dimensions, String timestampDimension )
    {
        super( dimensions, timestampDimension );
        this.reader = reader;
    }


    @Override public Iterator<InputRow> iterator()
    {
        return new JsonReaderIterator( this.reader, this.dimensions, this.timestampDimension );
    }


    /**
     * Iterator class
     */
    private class JsonReaderIterator implements Iterator<InputRow>, AutoCloseable
    {
        private final BufferedReader reader;
        private final List<String> dimensions;
        private final String timestampDimension;
        private final ObjectMapper objectMapper;
        private String nextLine;


        /**
         * Initialize a new JsonReaderIterator
         * @param reader the reader
         * @param dimensions the list of dimensions
         * @param timestampDimension the time stamp dimension
         */
        public JsonReaderIterator( Reader reader, List<String> dimensions, String timestampDimension )
        {
            this.reader = new BufferedReader( reader );
            this.dimensions = dimensions;
            this.timestampDimension = timestampDimension;
            this.objectMapper = new ObjectMapper();
        }


        /**
         * Helper method to get the timestamp
         * @param map the map i.e data
         * @return the timestamp in the data if it is present, or the default value
         */
        private Long getTimestamp( Map<String, Object> map )
        {
            if ( timestampDimension == null ) {
                return 1L;
            } else {
                return Long.valueOf( map.get( timestampDimension ).toString() );
            }
        }


        @Override public boolean hasNext()
        {
            try {
                if ( nextLine == null && ( nextLine = reader.readLine() ) == null ) {
                    close();
                    return false;
                } else {
                    return true;
                }
            } catch ( IOException e ) {
                e.printStackTrace();
                try {
                    close();
                } catch ( IOException e1 ) {
                    e1.printStackTrace();
                }
                return false;
            }
        }


        @Override public InputRow next()
        {
            if ( !hasNext() ) {
                return null;
            }
            Map<String, Object> map = null;
            try {
                map = this.objectMapper.readValue( nextLine, TYPE_REFERENCE );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            nextLine = null;
            if ( map == null ) {
                return next();
            }
            return new MapBasedInputRow( getTimestamp( map ), dimensions, map );
        }


        @Override public void remove()
        {
            throw new UnsupportedOperationException();
        }


        @Override public void close() throws IOException
        {
            this.reader.close();
        }
    }
}
