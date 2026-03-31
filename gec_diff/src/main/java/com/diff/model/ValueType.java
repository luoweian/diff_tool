package com.diff.model;

public enum ValueType {
    INT32(1),
    INT64(2),
    UINT32(3),
    UINT64(4),
    FLOAT32(5),
    FLOAT64(6),
    BOOL(7),
    STRING(8),
    NULL_TYPE(9),
    BYTES(10),
    JSON(11);

    public final int code;

    ValueType(int code) {
        this.code = code;
    }

    public static ValueType fromCode(int code) {
        for (ValueType v : values()) {
            if (v.code == code) return v;
        }
        return NULL_TYPE;
    }
}
