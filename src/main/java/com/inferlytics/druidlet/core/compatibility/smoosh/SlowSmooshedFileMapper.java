/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.core.compatibility.smoosh;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.inferlytics.druidlet.util.Utils;
import com.metamx.common.ISE;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Cloned from com.metamx.common.io.smoosh.SmooshedFileMapper
 * Removes Memory Mapped File dependency to make it work on Windows and uses regular FileInputStreams
 *
 * @author Sriram
 * @since 4/28/2016
 */
public class SlowSmooshedFileMapper implements Closeable {
    public static SlowSmooshedFileMapper load(File baseDir) throws IOException {
        File metaFile = SlowFileSmoosher.metaFile(baseDir);

        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(metaFile), Charsets.UTF_8));

            String line = in.readLine();
            if (line == null) {
                throw new ISE("First line should be version,maxChunkSize,numChunks, got null.");
            }

            String[] splits = line.split(",");
            if (!"v1".equals(splits[0])) {
                throw new ISE("Unknown version[%s], v1 is all I know.", splits[0]);
            }
            if (splits.length != 3) {
                throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
            }
            final Integer numFiles = Integer.valueOf(splits[2]);
            List<File> outFiles = Lists.newArrayListWithExpectedSize(numFiles);

            for (int i = 0; i < numFiles; ++i) {
                outFiles.add(SlowFileSmoosher.makeChunkFile(baseDir, i));
            }

            Map<String, Metadata> internalFiles = Maps.newTreeMap();
            while ((line = in.readLine()) != null) {
                splits = line.split(",");

                if (splits.length != 4) {
                    throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
                }
                internalFiles.put(
                        splits[0],
                        new Metadata(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3]))
                );
            }

            return new SlowSmooshedFileMapper(outFiles, internalFiles);
        } finally {
            Closeables.close(in, false);
        }
    }

    private final List<File> outFiles;
    private final Map<String, Metadata> internalFiles;
    private final List<ByteBuffer> buffersList = Lists.newArrayList();

    private SlowSmooshedFileMapper(
            List<File> outFiles,
            Map<String, Metadata> internalFiles
    ) {
        this.outFiles = outFiles;
        this.internalFiles = internalFiles;
    }

    public Set<String> getInternalFilenames() {
        return internalFiles.keySet();
    }

    public ByteBuffer mapFile(String name) throws IOException {
        final Metadata metadata = internalFiles.get(name);
        if (metadata == null) {
            return null;
        }

        final int fileNum = metadata.getFileNum();
        while (buffersList.size() <= fileNum) {
            buffersList.add(null);
        }

        ByteBuffer buffer = buffersList.get(fileNum);
        if (buffer == null) {
            buffer = Utils.readFile(outFiles.get(fileNum));
            buffersList.set(fileNum, buffer);
        }

        ByteBuffer retVal = buffer.duplicate();
        retVal.position(metadata.getStartOffset()).limit(metadata.getEndOffset());
        return retVal.slice();
    }

    @Override
    public void close() throws IOException {

    }
}
