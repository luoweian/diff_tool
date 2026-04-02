#include "diff_value.h"

#include <sstream>
#include <cstring>
#include <stdexcept>

namespace diff {
    
const char* DiffValue::TypeName() const {
    switch (type) {
        case ValueType::INT32:     return "int32";
        case ValueType::INT64:     return "int64";
        case ValueType::UINT32:    return "uint32";
        case ValueType::UINT64:    return "uint64";
        case ValueType::FLOAT32:   return "float32";
        case ValueType::FLOAT64:   return "float64";
        case ValueType::BOOL:      return "bool";
        case ValueType::STRING:    return "string";
        case ValueType::BYTES:     return "bytes";
        case ValueType::JSON:      return "json";
        case ValueType::NULL_TYPE: return "null";
    }
    return "unknown";
}
std::string DiffValue::DebugString() const {
    std::ostringstream oss;
    switch (type) {
        case ValueType::INT32:     oss << "int32("   << i32 << ")"; break;
        case ValueType::INT64:     oss << "int64("   << i64 << ")"; break;
        case ValueType::UINT32:    oss << "uint32("  << u32 << ")"; break;
        case ValueType::UINT64:    oss << "uint64("  << u64 << ")"; break;
        case ValueType::FLOAT32:   oss << "float32(" << f32 << ")"; break;
        case ValueType::FLOAT64:   oss << "float64(" << f64 << ")"; break;
        case ValueType::BOOL:      oss << "bool("    << (b ? "true" : "false") << ")"; break;
        case ValueType::STRING:    oss << "string(\"" << str << "\")"; break;
        case ValueType::BYTES:     oss << "bytes(len=" << str.size() << ")"; break;
        case ValueType::JSON:      oss << "json("    << str << ")"; break;
        case ValueType::NULL_TYPE: oss << "null"; break;
    }
    return oss.str();
}

// 序列化：数值类型用小端字节序，STRING/BYTES/JSON 直接存原始字节
std::string DiffValue::Serialize() const {
    std::string out;
    auto append_bytes = [&](const void* data, size_t len) {
        out.append(reinterpret_cast<const char*>(data), len);
    };

    switch (type) {
        case ValueType::INT32:     append_bytes(&i32, sizeof(i32)); break;
        case ValueType::INT64:     append_bytes(&i64, sizeof(i64)); break;
        case ValueType::UINT32:    append_bytes(&u32, sizeof(u32)); break;
        case ValueType::UINT64:    append_bytes(&u64, sizeof(u64)); break;
        case ValueType::FLOAT32:   append_bytes(&f32, sizeof(f32)); break;
        case ValueType::FLOAT64:   append_bytes(&f64, sizeof(f64)); break;
        case ValueType::BOOL:      { uint8_t v = b ? 1 : 0; append_bytes(&v, 1); break; }
        case ValueType::STRING:
        case ValueType::BYTES:
        case ValueType::JSON:      out = str; break;
        case ValueType::NULL_TYPE: break;
    }
    return out;
}

DiffValue DiffValue::Deserialize(ValueType type, const std::string& bytes) {
    DiffValue d;
    d.type = type;

    auto check_size = [&](size_t expected) {
        if (bytes.size() < expected) {
            throw std::runtime_error("DiffValue::Deserialize: insufficient bytes");
        }
    };

    switch (type) {
        case ValueType::INT32:
            check_size(sizeof(int32_t));
            std::memcpy(&d.i32, bytes.data(), sizeof(int32_t));
            break;
        case ValueType::INT64:
            check_size(sizeof(int64_t));
            std::memcpy(&d.i64, bytes.data(), sizeof(int64_t));
            break;
        case ValueType::UINT32:
            check_size(sizeof(uint32_t));
            std::memcpy(&d.u32, bytes.data(), sizeof(uint32_t));
            break;
        case ValueType::UINT64:
            check_size(sizeof(uint64_t));
            std::memcpy(&d.u64, bytes.data(), sizeof(uint64_t));
            break;
        case ValueType::FLOAT32:
            check_size(sizeof(float));
            std::memcpy(&d.f32, bytes.data(), sizeof(float));
            break;
        case ValueType::FLOAT64:
            check_size(sizeof(double));
            std::memcpy(&d.f64, bytes.data(), sizeof(double));
            break;
        case ValueType::BOOL:
            check_size(1);
            d.b = (static_cast<uint8_t>(bytes[0]) != 0);
            break;
        case ValueType::STRING:
        case ValueType::BYTES:
        case ValueType::JSON:
            d.str = bytes;
            break;
        case ValueType::NULL_TYPE:
            break;
    }
    return d;
}

} // namespace diff
