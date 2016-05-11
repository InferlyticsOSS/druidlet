/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.core.compatibility.smoosh;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metamx.common.collect.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Cloned from com.metamx.common.io.smoosh.Smoosh
 * Removes Memory Mapped File dependency to make it work on Windows and uses regular FileInputStreams
 *
 * @author Sriram
 * @since 4/28/2016
 */
public class SlowSmoosh {
    public static Map<String, File> smoosh(File inDir, File outDir) throws IOException {
        final List<File> files = Arrays.asList(inDir.listFiles());
        return smoosh(
                inDir,
                outDir,
                Utils.zipMap(
                        Iterables.transform(
                                files,
                                new Function<File, String>() {
                                    @Override
                                    public String apply(File input) {
                                        return input.getName();
                                    }
                                }
                        ),
                        files
                )
        );
    }

    public static Map<String, File> smoosh(File inDir, File outDir, Map<String, File> filesToSmoosh) throws IOException {
        SlowFileSmoosher smoosher = new SlowFileSmoosher(outDir);
        try {
            for (Map.Entry<String, File> entry : filesToSmoosh.entrySet()) {
                smoosher.add(entry.getKey(), entry.getValue());
            }
        } finally {
            smoosher.close();
        }

        return filesToSmoosh;
    }

    public static void smoosh(File outDir, Map<String, ByteBuffer> bufferstoSmoosh)
            throws IOException {
        SlowFileSmoosher smoosher = new SlowFileSmoosher(outDir);
        try {
            for (Map.Entry<String, ByteBuffer> entry : bufferstoSmoosh.entrySet()) {
                smoosher.add(entry.getKey(), entry.getValue());
            }
        } finally {
            smoosher.close();
        }
    }

    public static SlowSmooshedFileMapper map(File inDir) throws IOException {
        return SlowSmooshedFileMapper.load(inDir);
    }
}
