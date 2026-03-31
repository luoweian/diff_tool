#include <gtest/gtest.h>
#include "gec_diff.h"

using namespace diff;

// =========================================================================
// 用户接口测试
// 参数顺序：request_id, group, key, value/fields [, tags]
// =========================================================================

// 场景 1：推荐系统 CTR 打分，单值对比
TEST(UserWriteTest, SingleValueCtrScore) {
    Tags tags = {{"scene", "homepage"}, {"uid", "10086"}};

    diff::Write("rec_req_001", Group::BASE, "ctr_score", 0.312, tags);
    diff::Write("rec_req_001", Group::TEST, "ctr_score", 0.328, tags);
}

// 场景 2：特征工程迁移，批量多字段 diff
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
        {"city_level",  2},
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

// 场景 4：tags 可选
TEST(UserWriteTest, WriteWithoutTags) {
    diff::Write("req_004", Group::BASE, "score", 0.5);
    diff::Write("req_004", Group::TEST, "score", 0.6);

    diff::Write("req_004", Group::BASE, "features", {
        {"a", 1},
        {"b", 2.0},
    });
}

// 场景 5：未启用时直接返回，不崩溃
TEST(UserWriteTest, DisabledPathDoesNotCrash) {
    diff::Write("req_005", Group::BASE, "key", 1.0);
    diff::Write("req_005", Group::TEST, "key", 2.0);
    diff::Write("req_005", Group::BASE, "batch", {{"a", 1}, {"b", true}});
}
