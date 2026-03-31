#pragma once

/**
 * GecDiff — 轻量级 A/B Diff 埋点库
 *
 * 环境变量：
 *   GEC_DIFF_ENABLED=1            启用（默认关闭）
 *   GEC_DIFF_SERVICE_NAME=svc     服务名
 *   GEC_DIFF_REGION=ROW           数据域（ROW/EU/TTP，默认 ROW）
 *   GEC_DIFF_SAMPLE_RATE=1.0      采样率 0.0~1.0
 *   GEC_DIFF_THREAD_POOL_SIZE=4   写线程数（默认 2）
 *   GEC_DIFF_QUEUE_SIZE=10000     队列深度（默认 10000）
 *
 * 用法：
 *   // 单值
 *   diff::Write(request_id, Group::BASE, "ctr_score", 0.12, tags);
 *   diff::Write(request_id, Group::TEST, "ctr_score", 0.13, tags);
 *
 *   // 批量（直接写原生值，无需显式类型转换）
 *   diff::Write(request_id, Group::BASE, "feature", {
 *       {"ctr",    0.12},
 *       {"uid",    42},
 *       {"is_vip", true},
 *   }, tags);
 */

#include "diff_types.h"
#include "diff_value.h"   // 内部类型，用户无需直接使用

#include <string>
#include <vector>

namespace diff {

// 内部类型：value 字段列表（DiffValue 有隐式构造，用户直接写原生值即可）
using FieldList = std::vector<std::pair<std::string, DiffValue>>;

// =========================================================================
// GecDiff — 内部单例，负责 MQ 写入（用户不直接使用此类）
// =========================================================================
class AsyncWriter;

class GecDiff {
public:
    static bool IsEnabled();

    static void WriteOne(const std::string& request_id, Group group,
                         const std::string& key, DiffValue value,
                         const Tags& tags);

    static void WriteMany(const std::string& request_id, Group group,
                          const std::string& key, const FieldList& fields,
                          const Tags& tags);

private:
    GecDiff() = default;
    static GecDiff* GetInstance();

    AsyncWriter* writer_      = nullptr;
    std::string  service_name_;
    std::string  region_;
    float        sample_rate_ = 1.0f;
};

// =========================================================================
// 用户接口：diff::Write
// 参数顺序：request_id, group, key, value/fields [, tags]
// =========================================================================

// 单值写入（任意原生类型自动转换，无需指定 DiffValue）
template <typename T>
inline void Write(const std::string& request_id,
                  Group              group,
                  const std::string& key,
                  T&&                value,
                  const Tags&        tags = {}) {
    GecDiff::WriteOne(request_id, group, key,
                      DiffValue(std::forward<T>(value)), tags);
}

// 批量写入（value 直接写原生值，隐式构造，无需显式类型转换）
inline void Write(const std::string& request_id,
                  Group              group,
                  const std::string& key,
                  const FieldList&   fields,
                  const Tags&        tags = {}) {
    GecDiff::WriteMany(request_id, group, key, fields, tags);
}

} // namespace diff
