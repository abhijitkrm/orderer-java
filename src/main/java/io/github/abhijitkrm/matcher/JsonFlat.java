//! Minimal flat-JSON reader for vector/corpus lines — {"k":num,"k":"str"}
//! objects only (no nesting, no escapes needed for our grammar).
package io.github.abhijitkrm.matcher;

public final class JsonFlat {
    private JsonFlat() {}

    /// Value of `key` in a flat JSON object line, or null.
    public static String get(String line, String key) {
        String pat = "\"" + key + "\":";
        int start = line.indexOf(pat);
        if (start < 0) return null;
        String rest = line.substring(start + pat.length());
        if (rest.startsWith("\"")) {
            int end = rest.indexOf('"', 1);
            return end < 0 ? null : rest.substring(1, end);
        }
        int end = rest.length();
        for (int i = 0; i < rest.length(); i++) {
            char c = rest.charAt(i);
            if (c == ',' || c == '}') { end = i; break; }
        }
        return rest.substring(0, end).trim();
    }

    public static Long getLong(String line, String key) {
        String s = get(line, key);
        if (s == null) return null;
        try { return Long.parseLong(s); } catch (NumberFormatException e) { return null; }
    }
}
