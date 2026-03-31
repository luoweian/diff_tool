package com.diff.model;

import java.util.Map;

/**
 * Flink 内部流转的数据结构，对应 C++ PendingMessage 反序列化结果。
 */
public class DiffRecord {

    public enum Side { OLD, NEW }

    public String              requestId;
    public String              service;
    public String              region;
    public Side                side;
    public String              key;
    public Map<String, FieldValue> fields;  // fieldName -> value
    public Map<String, String> tags;
    public long                timestampMs;

    // ---- 单个字段值 ----
    public static class FieldValue {
        public ValueType type;
        public byte[]    rawBytes;

        // 反序列化后的 Java 值（按类型填充其中一个）
        public Integer  i32;
        public Long     i64;
        public Float    f32;
        public Double   f64;
        public Boolean  b;
        public String   str; // STRING / BYTES / JSON

        @Override
        public String toString() {
            switch (type) {
                case INT32:  return "int32(" + i32 + ")";
                case INT64:  return "int64(" + i64 + ")";
                case UINT32: return "uint32(" + i64 + ")"; // uint32 用 long 表示
                case UINT64: return "uint64(" + i64 + ")";
                case FLOAT32: return "float32(" + f32 + ")";
                case FLOAT64: return "float64(" + f64 + ")";
                case BOOL:   return "bool(" + b + ")";
                case STRING: return "string(\"" + str + "\")";
                case JSON:   return "json(" + str + ")";
                case BYTES:  return "bytes(len=" + (rawBytes != null ? rawBytes.length : 0) + ")";
                case NULL_TYPE: return "null";
                default:     return "unknown";
            }
        }
    }
}
