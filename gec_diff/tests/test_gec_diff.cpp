#include <gtest/gtest.h>
#include "gec_diff.h"
#include "async_writer.h"

#include <mutex>
#include <vector>

using namespace diff;

// =========================================================================
// Mock MQ Producer
// =========================================================================
class MockMQProducer : public MQProducer {
public:
    bool Send(const std::string& topic,
              const std::string& payload,
              const std::string& request_id) override {
        std::lock_guard<std::mutex> lock(mu_);
        messages_.push_back({topic, payload, request_id});
        return true;
    }
    struct Msg { std::string topic; std::string payload; std::string request_id; };
    std::vector<Msg> messages_;
    std::mutex mu_;
};

// =========================================================================
// AsyncWriter 底层测试
// =========================================================================
class AsyncWriterTest : public ::testing::Test {
protected:
    void SetUp() override {
        mock_ptr_ = new MockMQProducer();
        WriterConfig cfg;
        cfg.service_name     = "test_service";
        cfg.queue_size       = 100;
        cfg.thread_pool_size = 1;
        writer_ = std::make_unique<AsyncWriter>(cfg,
                      std::unique_ptr<MockMQProducer>(mock_ptr_));
    }
    MockMQProducer*              mock_ptr_ = nullptr;
    std::unique_ptr<AsyncWriter> writer_;
};

TEST_F(AsyncWriterTest, GroupTopicRouting) {
    PendingMessage base_msg;
    base_msg.request_id   = "req_001";
    base_msg.service      = "test_service";
    base_msg.region       = "ROW";
    base_msg.group        = Group::BASE;
    base_msg.key          = "ctr_score";
    base_msg.fields       = {{"", 0.12}};
    base_msg.timestamp_ms = 0;

    PendingMessage test_msg = base_msg;
    test_msg.group  = Group::TEST;
    test_msg.fields = {{"", 0.13}};

    EXPECT_TRUE(writer_->Enqueue(base_msg));
    EXPECT_TRUE(writer_->Enqueue(test_msg));
    writer_->Flush();

    EXPECT_EQ(mock_ptr_->messages_.size(), 2u);
    EXPECT_EQ(mock_ptr_->messages_[0].topic, "test_service.diff.base");
    EXPECT_EQ(mock_ptr_->messages_[1].topic, "test_service.diff.test");
}

TEST_F(AsyncWriterTest, QueueFullDropsMessages) {
    auto* small_mock = new MockMQProducer();
    WriterConfig small_cfg;
    small_cfg.service_name     = "svc";
    small_cfg.queue_size       = 2;
    small_cfg.thread_pool_size = 1;
    AsyncWriter small_writer(small_cfg,
                              std::unique_ptr<MockMQProducer>(small_mock));

    PendingMessage msg;
    msg.request_id = "req_x"; msg.service = "svc"; msg.region = "ROW";
    msg.group = Group::BASE; msg.key = "k"; msg.timestamp_ms = 0;
    for (int i = 0; i < 10; ++i) small_writer.Enqueue(msg);
    EXPECT_GT(small_writer.DropCount(), 0u);
}

TEST_F(AsyncWriterTest, ThreadCount) {
    EXPECT_EQ(writer_->ThreadCount(), 1);
}

// =========================================================================
// 用户接口测试
// 参数顺序：request_id, group, key, value/fields [, tags]
// 用户只需传原生值，无需了解 DiffValue 等内部类型
// =========================================================================

// 场景 1：推荐系统 CTR 打分，单值对比
TEST(UserWriteTest, SingleValueCtrScore) {
    Tags tags = {{"scene", "homepage"}, {"uid", "10086"}};

    diff::Write("rec_req_001", Group::BASE, "ctr_score", 0.312, tags);
    diff::Write("rec_req_001", Group::TEST, "ctr_score", 0.328, tags);
}

// 场景 2：特征工程迁移，批量多字段 diff
// value 直接写原生值，无需显式类型声明
TEST(UserWriteTest, BatchFeatureMapDiff) {
    Tags tags = {{"region", "ROW"}};

    diff::Write("feat_req_002", Group::BASE, "feature", {
        {"age_bucket",  3},
        {"city_level",  1},
        {"is_new_user", false},
        {"item_price",  99.9},
    }, tags);

    diff::Write("feat_req_002", Group::TEST, "feature", {
        {"age_bucket",  3},
        {"city_level",  2},      // 变化
        {"is_new_user", false},
        {"item_price",  99.9},
    }, tags);
}

// 场景 3：不同原生类型自动推导
TEST(UserWriteTest, MixedNativeTypes) {
    diff::Write("req_003", Group::BASE, "score",   0.5f);
    diff::Write("req_003", Group::BASE, "count",   100);
    diff::Write("req_003", Group::BASE, "enabled", true);
    diff::Write("req_003", Group::BASE, "label",   "abc");
}

// 场景 4：tags 可选，不传时正常工作
TEST(UserWriteTest, WriteWithoutTags) {
    diff::Write("req_004", Group::BASE, "score", 0.5);
    diff::Write("req_004", Group::TEST, "score", 0.6);

    diff::Write("req_004", Group::BASE, "features", {
        {"a", 1},
        {"b", 2.0},
    });
}

// 场景 5：未启用时 Write 直接返回，不崩溃（零开销路径）
TEST(UserWriteTest, DisabledPathDoesNotCrash) {
    diff::Write("req_005", Group::BASE, "key", 1.0);
    diff::Write("req_005", Group::TEST, "key", 2.0);
    diff::Write("req_005", Group::BASE, "batch", {{"a", 1}, {"b", true}});
}
