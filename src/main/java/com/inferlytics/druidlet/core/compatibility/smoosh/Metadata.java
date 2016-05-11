/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.core.compatibility.smoosh;

/**
 * @author Sriram
 * @since 4/28/2016
 */
public class Metadata {
    private final int fileNum;
    private final int startOffset;
    private final int endOffset;

    Metadata(
            int fileNum,
            int startOffset,
            int endOffset
    ) {
        this.fileNum = fileNum;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public int getFileNum() {
        return fileNum;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }
}
