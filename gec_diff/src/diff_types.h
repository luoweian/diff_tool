#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace diff {

// 实验分组
enum class Group {
    BASE,  // 对照组（旧逻辑）
    TEST,  // 实验组（新逻辑）
};

// Diff 比较模式（Flink 侧使用）
enum class DiffMode { STRICT, LOOSE };

// 数组 Diff 模式（Flink 侧使用）
enum class ArrayDiffMode { ORDERED, UNORDERED };

// Tag map
using Tags = std::unordered_map<std::string, std::string>;

// Diff 操作类型（Flink 输出使用）
enum class DiffOp { EQUAL, VALUE_CHANGE, TYPE_CHANGE, ADDED, REMOVED };

} // namespace diff
