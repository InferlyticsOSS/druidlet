/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.core.compatibility.smoosh;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import com.inferlytics.druidlet.util.Utils;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.io.smoosh.SmooshedWriter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Cloned from com.metamx.common.io.smoosh.FileSmoosher
 * Removes Memory Mapped File dependency to make it work on Windows and uses regular FileInputStreams
 *
 * @author Sriram
 * @since 4/28/2016
 */
public class SlowFileSmoosher implements Closeable {
    private static final String FILE_EXTENSION = "smoosh";
    private static final Joiner joiner = Joiner.on(",");

    private final File baseDir;
    private final int maxChunkSize;

    private final List<File> outFiles = Lists.newArrayList();
    private final Map<String, Metadata> internalFiles = Maps.newTreeMap();

    private Outer currOut = null;

    public SlowFileSmoosher(
            File baseDir
    ) {
        this(baseDir, Integer.MAX_VALUE);
    }

    public SlowFileSmoosher(
            File baseDir,
            int maxChunkSize
    ) {
        this.baseDir = baseDir;
        this.maxChunkSize = maxChunkSize;

        Preconditions.checkArgument(maxChunkSize > 0, "maxChunkSize must be a positive value.");
    }

    private SlowFileSmoosher(
            File baseDir,
            int maxChunkSize,
            List<File> outFiles,
            Map<String, Metadata> internalFiles
    ) {
        this.baseDir = baseDir;
        this.maxChunkSize = maxChunkSize;
        this.outFiles.addAll(outFiles);
        this.internalFiles.putAll(internalFiles);
    }

    public Set<String> getInternalFilenames() {
        return internalFiles.keySet();
    }

    public void add(File fileToAdd) throws IOException {
        add(fileToAdd.getName(), Utils.readFile(fileToAdd));
    }

    public void add(String name, File fileToAdd) throws IOException {
        add(name, Utils.readFile(fileToAdd));
    }

    public void add(String name, ByteBuffer bufferToAdd) throws IOException {
        add(name, Arrays.asList(bufferToAdd));
    }

    public void add(String name, List<ByteBuffer> bufferToAdd) throws IOException {
        if (name.contains(",")) {
            throw new IAE("Cannot have a comma in the name of a file, got[%s].", name);
        }

        if (internalFiles.get(name) != null) {
            throw new IAE("Cannot add files of the same name, already have [%s]", name);
        }

        long size = 0;
        for (ByteBuffer buffer : bufferToAdd) {
            size += buffer.remaining();
        }

        SmooshedWriter out = addWithSmooshedWriter(name, size);

        try {
            for (ByteBuffer buffer : bufferToAdd) {
                out.write(buffer);
            }
        } finally {
            CloseQuietly.close(out);
        }
    }

    public SmooshedWriter addWithSmooshedWriter(final String name, final long size) throws IOException {
        if (size > maxChunkSize) {
            throw new IAE("Asked to add buffers[%,d] larger than configured max[%,d]", size, maxChunkSize);
        }
        if (currOut == null) {
            currOut = getNewCurrOut();
        }
        if (currOut.bytesLeft() < size) {
            Closeables.close(currOut, false);
            currOut = getNewCurrOut();
        }

        final int startOffset = currOut.getCurrOffset();

        return new SmooshedWriter() {
            private boolean open = true;
            private long bytesWritten = 0;

            @Override
            public int write(InputStream in) throws IOException {
                return verifySize(currOut.write(in));
            }

            @Override
            public int write(ByteBuffer in) throws IOException {
                return verifySize(currOut.write(in));
            }

            private int verifySize(int bytesWrittenInChunk) throws IOException {
                bytesWritten += bytesWrittenInChunk;

                if (bytesWritten != currOut.getCurrOffset() - startOffset) {
                    throw new ISE("WTF? Perhaps there is some concurrent modification going on?");
                }
                if (bytesWritten > size) {
                    throw new ISE("Wrote[%,d] bytes for something of size[%,d].  Liar!!!", bytesWritten, size);
                }

                return bytesWrittenInChunk;
            }

            @Override
            public boolean isOpen() {
                return open;
            }

            @Override
            public void close() throws IOException {
                open = false;
                internalFiles.put(name, new Metadata(currOut.getFileNum(), startOffset, currOut.getCurrOffset()));

                if (bytesWritten != currOut.getCurrOffset() - startOffset) {
                    throw new ISE("WTF? Perhaps there is some concurrent modification going on?");
                }
                if (bytesWritten != size) {
                    throw new IOException(
                            String.format("Expected [%,d] bytes, only saw [%,d], potential corruption?", size, bytesWritten)
                    );
                }
            }
        };
    }

    @Override
    public void close() throws IOException {
        Closeables.close(currOut, false);

        File metaFile = metaFile(baseDir);

        Writer out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(metaFile), Charsets.UTF_8));
            out.write(String.format("v1,%d,%d", maxChunkSize, outFiles.size()));
            out.write("\n");

            for (Map.Entry<String, Metadata> entry : internalFiles.entrySet()) {
                final Metadata metadata = entry.getValue();
                out.write(
                        joiner.join(
                                entry.getKey(),
                                metadata.getFileNum(),
                                metadata.getStartOffset(),
                                metadata.getEndOffset()
                        )
                );
                out.write("\n");
            }
        } finally {
            Closeables.close(out, false);
        }
    }

    private Outer getNewCurrOut() throws FileNotFoundException {
        final int fileNum = outFiles.size();
        File outFile = makeChunkFile(baseDir, fileNum);
        outFiles.add(outFile);
        return new Outer(fileNum, new BufferedOutputStream(new FileOutputStream(outFile)), maxChunkSize);
    }

    static File metaFile(File baseDir) {
        return new File(baseDir, String.format("meta.%s", FILE_EXTENSION));
    }

    static File makeChunkFile(File baseDir, int i) {
        return new File(baseDir, String.format("%05d.%s", i, FILE_EXTENSION));
    }

    public static class Outer implements SmooshedWriter {
        private final int fileNum;
        private final OutputStream out;
        private final int maxLength;

        private boolean open = true;
        private int currOffset = 0;

        Outer(int fileNum, OutputStream out, int maxLength) {
            this.fileNum = fileNum;
            this.out = out;
            this.maxLength = maxLength;
        }

        public int getFileNum() {
            return fileNum;
        }

        public int getCurrOffset() {
            return currOffset;
        }

        public int bytesLeft() {
            return maxLength - currOffset;
        }

        @Override
        public int write(ByteBuffer buffer) throws IOException {
            WritableByteChannel channel = Channels.newChannel(out);
            return addToOffset(channel.write(buffer));
        }

        @Override
        public int write(InputStream in) throws IOException {
            return addToOffset(Ints.checkedCast(ByteStreams.copy(in, out)));
        }

        public int addToOffset(int numBytesWritten) {
            if (numBytesWritten > bytesLeft()) {
                throw new ISE("Wrote more bytes[%,d] than available[%,d]. Don't do that.", numBytesWritten, bytesLeft());
            }

            currOffset += numBytesWritten;

            return numBytesWritten;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() throws IOException {
            open = false;
            out.close();
        }
    }
}
