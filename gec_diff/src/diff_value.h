#pragma once

#include <string>
#include <cstdint>
#include <stdexcept>

namespace diff {

enum class ValueType {
    INT32, INT64, UINT32, UINT64,
    FLOAT32, FLOAT64,
    BOOL,
    STRING,
    NULL_TYPE,
    BYTES,
    JSON,   // 递归结构，内部为 JSON 字符串
};

// 统一值容器：原生类型隐式构造，用户无需指定类型
struct DiffValue {
    ValueType   type = ValueType::NULL_TYPE;
    union { int32_t i32; int64_t i64; uint32_t u32; uint64_t u64;
            float f32; double f64; bool b; };
    std::string str;  // STRING / BYTES / JSON

    // ---- 隐式构造（原生类型自动转换）----
    DiffValue()             : type(ValueType::NULL_TYPE), i64(0) {}
    DiffValue(int32_t  v)   : type(ValueType::INT32),    i32(v) {}
    DiffValue(int64_t  v)   : type(ValueType::INT64),    i64(v) {}
    DiffValue(uint32_t v)   : type(ValueType::UINT32),   u32(v) {}
    DiffValue(uint64_t v)   : type(ValueType::UINT64),   u64(v) {}
    DiffValue(float    v)   : type(ValueType::FLOAT32),  f32(v) {}
    DiffValue(double   v)   : type(ValueType::FLOAT64),  f64(v) {}
    DiffValue(bool     v)   : type(ValueType::BOOL),     b(v)   {}
    DiffValue(std::string v): type(ValueType::STRING),   i64(0), str(std::move(v)) {}
    DiffValue(const char* v): type(ValueType::STRING),   i64(0), str(v)            {}

    // ---- JSON / Bytes 需显式标记类型 ----
    static DiffValue Json(std::string v)  { DiffValue d; d.type = ValueType::JSON;  d.str = std::move(v); return d; }
    static DiffValue Bytes(std::string v) { DiffValue d; d.type = ValueType::BYTES; d.str = std::move(v); return d; }

    const char* TypeName() const;
    std::string DebugString() const;
    std::string Serialize() const;
    static DiffValue Deserialize(ValueType type, const std::string& bytes);
};

} // namespace diff
