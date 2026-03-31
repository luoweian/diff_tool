#pragma once

#include "diff_types.h"
#include "diff_value.h"

#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>

namespace diff {

using FieldList = std::vector<std::pair<std::string, DiffValue>>;

// 单条 MQ 消息的内存表示（序列化前，仅供内部使用）
struct PendingMessage {
    std::string request_id;
    std::string service;
    std::string region;
    Group       group;    // BASE 或 TEST
    std::string key;
    FieldList   fields;
    Tags        tags;
    int64_t     timestamp_ms;
};

// MQ 写入接口（可注入 mock 用于测试）
class MQProducer {
public:
    virtual ~MQProducer() = default;
    virtual bool Send(const std::string& topic,
                      const std::string& payload,
                      const std::string& request_id) = 0;
};

// 写入器配置（内部使用，不暴露给用户）
struct WriterConfig {
    std::string service_name;
    int         queue_size        = 10000;
    int         thread_pool_size  = 1;
};

// 异步写入器：内存队列 + 后台线程池批量 flush 到 MQ
class AsyncWriter {
public:
    explicit AsyncWriter(WriterConfig config, std::unique_ptr<MQProducer> producer);
    ~AsyncWriter();

    // 将消息推入内存队列，非阻塞；队列满时丢弃并递增 drop_count_
    bool Enqueue(PendingMessage msg);

    // 等待队列清空（测试用）
    void Flush();

    uint64_t DropCount()   const { return drop_count_.load(); }
    int      ThreadCount() const { return static_cast<int>(workers_.size()); }

private:
    void        WorkerLoop();
    std::string Serialize(const PendingMessage& msg) const;

    WriterConfig                config_;
    std::unique_ptr<MQProducer> producer_;

    std::queue<PendingMessage>  queue_;
    std::mutex                  mu_;
    std::condition_variable     cv_;
    std::atomic<bool>           stop_{false};
    std::vector<std::thread>    workers_;

    std::atomic<uint64_t>       drop_count_{0};

    static constexpr int        kBatchSize = 64;
};

} // namespace diff
