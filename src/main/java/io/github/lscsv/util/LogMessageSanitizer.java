package io.github.lscsv.util;

import java.util.regex.Pattern;

public final class LogMessageSanitizer {

    private static final Pattern SANITIZER = Pattern.compile("[\r\n]");
    
    public static String sanitize(String value) {
        return value == null ? "" : value.replaceAll("\n", "").
                replaceAll("\r","").
                replaceAll("\\R","");
    }

    public static String sanitizeWithPattern(String value) {
        return value == null ? "" : SANITIZER.matcher(value).replaceAll("");
    }
}
