package com.diff.model;

import java.util.Map;

/**
 * 单条字段 Diff 结果，写入 ClickHouse / Hive。
 */
public class DiffResult {

    public enum Op {
        EQUAL,
        VALUE_CHANGE,
        TYPE_CHANGE,
        ADDED,
        REMOVED,
    }

    public String              requestId;
    public String              service;
    public String              region;
    public String              key;
    public String              fieldPath;   // JSON 路径，如 /item_ctr_7d

    public Op                  op;
    public String              oldType;
    public String              newType;
    public String              oldValue;    // DebugString
    public String              newValue;    // DebugString
    public double              absDiff;     // 仅数值类型有意义

    public Map<String, String> tags;
    public long                timestampMs;

    @Override
    public String toString() {
        return String.format(
            "[%s] %s | %s | %-20s | %-12s → %-12s | op=%-12s | absDiff=%.6f | tags=%s",
            service, region, requestId, fieldPath,
            oldValue, newValue, op, absDiff, tags
        );
    }
}
