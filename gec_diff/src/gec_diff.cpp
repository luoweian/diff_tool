#include "gec_diff.h"
#include "async_writer.h"

#include <chrono>
#include <cstdlib>
#include <mutex>
#include <random>

namespace diff {

// ---- Stub MQ producer（生产环境替换为真实 BMQ/Kafka SDK）----
class StubMQProducer : public MQProducer {
public:
    bool Send(const std::string& /*topic*/, const std::string& /*payload*/) override {
        return true;
    }
};

// =========================================================================
// 内部辅助
// =========================================================================
namespace {

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
// 懒初始化单例（线程安全）
// GEC_DIFF_ENABLED != "1" 时返回 nullptr，Write 立即退出
// =========================================================================
GecDiff* GecDiff::GetInstance() {
    static GecDiff*       instance = nullptr;
    static bool           checked  = false;
    static std::once_flag flag;

    if (checked) return instance;  // 快速路径，无锁

    std::call_once(flag, [] {
        checked = true;
        const char* enabled = std::getenv("GEC_DIFF_ENABLED");
        if (!enabled || std::string(enabled) != "1") return;

        WriterConfig cfg;
        cfg.service_name     = GetEnv("GEC_DIFF_SERVICE_NAME", "unknown_service");
        cfg.queue_size       = GetEnvInt("GEC_DIFF_QUEUE_SIZE", 10000);
        cfg.thread_pool_size = GetEnvInt("GEC_DIFF_THREAD_POOL_SIZE", 2);

        auto* obj          = new GecDiff();
        obj->service_name_ = cfg.service_name;
        obj->region_       = GetEnv("GEC_DIFF_REGION", "ROW");
        obj->sample_rate_  = GetEnvFloat("GEC_DIFF_SAMPLE_RATE", 1.0f);
        obj->writer_       = new AsyncWriter(std::move(cfg),
                                             std::make_unique<StubMQProducer>());
        instance = obj;
    });
    return instance;
}

// =========================================================================
// MQ 写入层（不涉及类型转换，只组装 PendingMessage 并入队）
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

    PendingMessage msg;
    msg.request_id   = request_id;
    msg.service      = inst->service_name_;
    msg.region       = inst->region_;
    msg.group        = group;
    msg.key          = key;
    msg.fields       = {{"", std::move(value)}};
    msg.tags         = tags;
    msg.timestamp_ms = NowMs();
    inst->writer_->Enqueue(std::move(msg));
}

void GecDiff::WriteMany(const std::string& request_id,
                        Group              group,
                        const std::string& key,
                        const FieldList&   fields,
                        const Tags&        tags) {
    GecDiff* inst = GetInstance();
    if (!inst || fields.empty()) return;
    if (!ShouldSample(inst->sample_rate_)) return;

    PendingMessage msg;
    msg.request_id   = request_id;
    msg.service      = inst->service_name_;
    msg.region       = inst->region_;
    msg.group        = group;
    msg.key          = key;
    msg.fields       = fields;
    msg.tags         = tags;
    msg.timestamp_ms = NowMs();
    inst->writer_->Enqueue(std::move(msg));
}

} // namespace diff
