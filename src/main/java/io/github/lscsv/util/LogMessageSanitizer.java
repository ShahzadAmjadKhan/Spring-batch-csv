package io.github.lscsv.util;

public final class LogMessageSanitizer {

    public static String sanitize(String value) {
        return value == null ? "" : value.replaceAll("\n", "").
                replaceAll("\r","").
                replaceAll("\\R","");
    }
}
