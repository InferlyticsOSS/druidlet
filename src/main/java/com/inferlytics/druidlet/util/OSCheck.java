/*
 * Copyright (c) 2016 Inferlytics.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 */

package com.inferlytics.druidlet.util;

/**
 * Checks OS type
 *
 * @author Sriram
 * @since 4/28/2016
 */
public class OSCheck {
    private static String OS = System.getProperty("os.name").toLowerCase();

    public static boolean isUnix() {
        return (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0);
    }

    public static boolean isWindows() {
        return (OS.indexOf("win") >= 0);
    }

    public static boolean isMac() {
        return (OS.indexOf("mac") >= 0);
    }

    public static boolean isSolaris() {
        return (OS.indexOf("sunos") >= 0);
    }
}