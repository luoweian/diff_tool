#include <gtest/gtest.h>
#include "src/diff_value.cpp"

using namespace diff;

TEST(DiffValueTest, Int32RoundTrip) {
    DiffValue v = DiffValue(42);
    std::string bytes = v.Serialize();
    DiffValue v2 = DiffValue::Deserialize(ValueType::INT32, bytes);
    EXPECT_EQ(v2.i32, 42);
    EXPECT_EQ(v2.type, ValueType::INT32);
}

TEST(DiffValueTest, Float64RoundTrip) {
    DiffValue v = DiffValue(3.14159);
    std::string bytes = v.Serialize();
    DiffValue v2 = DiffValue::Deserialize(ValueType::FLOAT64, bytes);
    EXPECT_DOUBLE_EQ(v2.f64, 3.14159);
}

TEST(DiffValueTest, StringRoundTrip) {
    DiffValue v = DiffValue("hello_world");
    std::string bytes = v.Serialize();
    DiffValue v2 = DiffValue::Deserialize(ValueType::STRING, bytes);
    EXPECT_EQ(v2.str, "hello_world");
}

TEST(DiffValueTest, JsonRoundTrip) {
    std::string json = R"({"score":0.12,"tags":["vip"]})";
    DiffValue v = DiffValue::Json(json);
    std::string bytes = v.Serialize();
    DiffValue v2 = DiffValue::Deserialize(ValueType::JSON, bytes);
    EXPECT_EQ(v2.str, json);
    EXPECT_EQ(v2.type, ValueType::JSON);
}

TEST(DiffValueTest, BoolRoundTrip) {
    for (bool b : {true, false}) {
        DiffValue v = DiffValue(b);
        std::string bytes = v.Serialize();
        DiffValue v2 = DiffValue::Deserialize(ValueType::BOOL, bytes);
        EXPECT_EQ(v2.b, b);
    }
}

TEST(DiffValueTest, NullSerialize) {
    DiffValue v = DiffValue();
    std::string bytes = v.Serialize();
    EXPECT_TRUE(bytes.empty());
}

TEST(DiffValueTest, TypeName) {
    EXPECT_STREQ(DiffValue(1.0).TypeName(), "float64");
    EXPECT_STREQ(DiffValue::Json("{}").TypeName(), "json");
}

TEST(DiffValueTest, DebugString) {
    EXPECT_EQ(DiffValue(99).DebugString(), "int32(99)");
    EXPECT_EQ(DiffValue(true).DebugString(), "bool(true)");
}
