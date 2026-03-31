#include "gec_diff.h"

#include <cpputil/databusclient/include/client.h>

#include <chrono>
#include <cstdlib>
#include <mutex>
#include <random>
#include <string>

namespace diff {

// =========================================================================
// 序列化（二进制格式，Flink 侧对应 DiffRecordDeserializer）
// =========================================================================
namespace {

void WriteUint32(std::string& out, uint32_t v) {
    out.append(reinterpret_cast<const char*>(&v), 4);
}
void WriteString(std::string& out, const std::string& s) {
    WriteUint32(out, static_cast<uint32_t>(s.size()));
    out.append(s);
}
void WriteInt64(std::string& out, int64_t v) {
    out.append(reinterpret_cast<const char*>(&v), 8);
}

std::string Serialize(const std::string& request_id,
                      const std::string& service,
                      const std::string& region,
                      Group              group,
                      const std::string& key,
                      const FieldList&   fields,
                      const Tags&        tags,
                      int64_t            timestamp_ms) {
    std::string out;
    out.reserve(256);
    WriteString(out, request_id);
    WriteString(out, service);
    WriteString(out, region);
    out.push_back(static_cast<char>(group == Group::BASE ? 1 : 2));
    WriteString(out, key);
    WriteUint32(out, static_cast<uint32_t>(tags.size()));
    for (const auto& [k, v] : tags) {
        WriteString(out, k);
        WriteString(out, v);
    }
    WriteInt64(out, timestamp_ms);
    WriteUint32(out, static_cast<uint32_t>(fields.size()));
    for (const auto& [name, val] : fields) {
        WriteString(out, name);
        out.push_back(static_cast<char>(static_cast<int>(val.type)));
        WriteString(out, val.Serialize());
    }
    return out;
}

bool ShouldSample(float rate) {
    if (rate >= 1.0f) return true;
    if (rate <= 0.0f) return false;
    static thread_local std::mt19937 rng(std::random_device{}());
    static thread_local std::uniform_real_distribution<float> dist(0.0f, 1.0f);
    return dist(rng) < rate;
}

int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

const char* GetEnv(const char* key, const char* fallback = "") {
    const char* v = std::getenv(key);
    return (v && v[0]) ? v : fallback;
}

int GetEnvInt(const char* key, int fallback) {
    const char* v = std::getenv(key);
    if (v) { int n = std::atoi(v); if (n > 0) return n; }
    return fallback;
}

float GetEnvFloat(const char* key, float fallback) {
    const char* v = std::getenv(key);
    if (v) { float f = std::atof(v); if (f >= 0.0f) return f; }
    return fallback;
}

} // namespace

// =========================================================================
// 懒初始化单例
// =========================================================================
GecDiff* GecDiff::GetInstance() {
    static GecDiff*       instance = nullptr;
    static bool           checked  = false;
    static std::once_flag flag;

    if (checked) return instance;

    std::call_once(flag, [] {
        checked = true;
        const char* enabled = std::getenv("GEC_DIFF_ENABLED");
        if (!enabled || std::string(enabled) != "1") return;

        auto* obj          = new GecDiff();
        obj->service_name_ = GetEnv("GEC_DIFF_SERVICE_NAME", "unknown_service");
        obj->region_       = GetEnv("GEC_DIFF_REGION", "ROW");
        obj->sample_rate_  = GetEnvFloat("GEC_DIFF_SAMPLE_RATE", 1.0f);
        obj->base_client_  = std::make_shared<DatabusClient>(obj->service_name_ + ".diff.base");
        obj->test_client_  = std::make_shared<DatabusClient>(obj->service_name_ + ".diff.test");
        instance = obj;
    });
    return instance;
}

// =========================================================================
// MQ 写入层
// =========================================================================
bool GecDiff::IsEnabled() {
    return GetInstance() != nullptr;
}

void GecDiff::WriteOne(const std::string& request_id,
                       Group              group,
                       const std::string& key,
                       DiffValue          value,
                       const Tags&        tags) {
    GecDiff* inst = GetInstance();
    if (!inst) return;
    if (!ShouldSample(inst->sample_rate_)) return;

    auto payload = Serialize(request_id, inst->service_name_, inst->region_,
                             group, key, {{"", std::move(value)}}, tags, NowMs());
    auto& client = (group == Group::BASE) ? inst->base_client_ : inst->test_client_;
    client->send(payload.c_str(), request_id);
}

void GecDiff::WriteMany(const std::string& request_id,
                        Group              group,
                        const std::string& key,
                        const FieldList&   fields,
                        const Tags&        tags) {
    GecDiff* inst = GetInstance();
    if (!inst || fields.empty()) return;
    if (!ShouldSample(inst->sample_rate_)) return;

    auto payload = Serialize(request_id, inst->service_name_, inst->region_,
                             group, key, fields, tags, NowMs());
    auto& client = (group == Group::BASE) ? inst->base_client_ : inst->test_client_;
    client->send(payload.c_str(), request_id);
}

} // namespace diff
