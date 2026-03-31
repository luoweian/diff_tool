# GecDiff

轻量级 C++ A/B Diff 埋点库，用于验证代码迁移（特征工程、架构重构等）的等价性。

在对照组（旧逻辑）和实验组（新逻辑）的关键节点各埋一行 `diff::Write`，数据异步写入 MQ，由下游 Flink 任务完成 join 和 diff 计算，并输出监控指标。

## 架构

```
业务代码
  │  diff::Write(BASE, ...)   diff::Write(TEST, ...)
  ▼
GecDiff (C++ lib)
  │  异步队列 + 线程池
  ▼
BMQ topic: <service>.diff.base / <service>.diff.test
  │
  ▼
Flink Joiner（30s 滚动窗口 CoGroup Join）
  │
  ▼
diff 结果 + 监控指标（diff_rate / type_change_rate / join_miss_rate）
```

## 快速上手

### 1. 引入依赖

```python
# BUILD
deps = ['//gec_diff:gec_diff']
```

### 2. 配置环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `GEC_DIFF_ENABLED` | 启用开关，设为 `1` 才生效 | 关闭 |
| `GEC_DIFF_SERVICE_NAME` | 服务名，决定 MQ topic 前缀 | `unknown_service` |
| `GEC_DIFF_REGION` | 数据域（`ROW` / `EU` / `TTP`） | `ROW` |
| `GEC_DIFF_SAMPLE_RATE` | 采样率 `0.0~1.0` | `1.0` |
| `GEC_DIFF_THREAD_POOL_SIZE` | 异步写入线程数 | `2` |
| `GEC_DIFF_QUEUE_SIZE` | 内存队列深度，满时丢弃 | `10000` |

```bash
export GEC_DIFF_ENABLED=1
export GEC_DIFF_SERVICE_NAME=rank_service
export GEC_DIFF_REGION=ROW
```

### 3. 埋点

```cpp
#include "gec_diff.h"

using namespace diff;

// 单值对比
diff::Write(request_id, Group::BASE, "ctr_score", old_score, tags);
diff::Write(request_id, Group::TEST, "ctr_score", new_score, tags);

// 批量字段对比（直接写原生值，无需类型转换）
diff::Write(request_id, Group::BASE, "feature", {
    {"age_bucket",  3},
    {"city_level",  1},
    {"is_new_user", false},
    {"item_price",  99.9},
}, tags);

diff::Write(request_id, Group::TEST, "feature", {
    {"age_bucket",  3},
    {"city_level",  2},
    {"is_new_user", false},
    {"item_price",  99.9},
}, tags);
```

支持的原生类型：`int32_t` / `int64_t` / `uint32_t` / `uint64_t` / `float` / `double` / `bool` / `std::string` / `const char*`，以及 `DiffValue::Json(str)` 和 `DiffValue::Bytes(str)`。

### 4. Tags

`tags` 为可选的 `std::unordered_map<std::string, std::string>`，用于下游分组过滤：

```cpp
Tags tags = {{"scene", "homepage"}, {"uid", "10086"}};
```

## 设计说明

- **零开销禁用**：`GEC_DIFF_ENABLED` 未设为 `1` 时，`diff::Write` 第一行即返回，不涉及任何锁或内存分配。
- **懒初始化单例**：首次调用时通过 `std::call_once` 初始化，读取所有配置，之后快速路径无锁。
- **异步非阻塞**：写入操作仅将消息入队（< 0.1ms），后台线程池批量 flush 到 MQ。队列满时丢弃并计数，不阻塞业务线程。
- **类型转换与 MQ 写入分离**：`diff::Write` 模板负责 `T → DiffValue` 转换，`GecDiff::WriteOne/WriteMany` 只负责组装消息和入队。

## 构建与测试

需要安装 [Blade](https://github.com/chen3feng/blade-build) 和 LLVM。

```bash
# 编译 + 测试（使用 LLVM）
bash build.dev

# 单独编译
blade build //gec_diff:gec_diff --toolchain=clang

# 单独测试
blade test //gec_diff:gec_diff_test --toolchain=clang
```

## 目录结构

```
gec_diff/
├── BUILD
├── src/
│   ├── gec_diff.h        # 用户接口
│   ├── gec_diff.cpp      # 单例实现 + MQ 写入
│   ├── async_writer.h/cpp  # 异步队列 + 线程池
│   ├── diff_value.h/cpp    # 值类型封装
│   └── diff_types.h        # 枚举类型定义
└── tests/
    ├── test_gec_diff.cpp   # 用户接口 + 集成测试
    └── test_diff_value.cpp # DiffValue 单元测试
```
