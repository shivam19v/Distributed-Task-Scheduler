package dev.jobscheduler.handler;

import java.util.Map;

/** Thin typed accessor over the task's JSON params. */
public record Params(Map<String, Object> raw) {

    public String string(String key, String fallback) {
        Object v = raw.get(key);
        return v == null ? fallback : v.toString();
    }

    public int integer(String key, int fallback) {
        Object v = raw.get(key);
        if (v == null) return fallback;
        return v instanceof Number n ? n.intValue() : Integer.parseInt(v.toString());
    }

    public double decimal(String key, double fallback) {
        Object v = raw.get(key);
        if (v == null) return fallback;
        return v instanceof Number n ? n.doubleValue() : Double.parseDouble(v.toString());
    }
}
