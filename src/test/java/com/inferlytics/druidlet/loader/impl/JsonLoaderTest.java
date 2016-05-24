package com.inferlytics.druidlet.loader.impl;

import io.druid.data.input.InputRow;
import org.testng.annotations.Test;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.*;


/**
 * Tests for JsonLoader
 * Created by Samarth Bhargav on 5/15/16.
 */
public class JsonLoaderTest
{
    @Test public void testJsonLoader() throws Exception
    {
        JsonLoader jsonLoader = new JsonLoader(
            new InputStreamReader( JsonLoaderTest.class.getClassLoader().getResourceAsStream( "testJsonLoader.json" ) ),
            Arrays.asList( "dim1", "dim2", "dim3" ), "timestamp" );
        List<InputRow> inputRowList = new ArrayList<>();

        for ( InputRow inputRow : jsonLoader ) {
            inputRowList.add( inputRow );
        }

        assertEquals( 3, inputRowList.size() );

        for ( int i = 0; i < inputRowList.size(); i++ ) {
            InputRow inputRow = inputRowList.get( i );
            for ( String dim : inputRow.getDimensions() ) {
                assertEquals( String.valueOf( i + 1 ), inputRow.getDimension( dim ).get( 0 ) );
            }
        }
    }
}