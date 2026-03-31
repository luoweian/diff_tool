cmake_minimum_required(VERSION 3.14)
project(gec_diff VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---- 库目标 ----
add_library(gec_diff STATIC
    src/diff_value.cpp
    src/async_writer.cpp
    src/gec_diff.cpp
)

target_include_directories(gec_diff
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(gec_diff PRIVATE -Wall -Wextra -O2)

# ---- 可选：Protobuf 集成（取消注释后启用） ----
# find_package(Protobuf REQUIRED)
# target_link_libraries(gec_diff PUBLIC protobuf::libprotobuf)
# protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS proto/diff_message.proto)
# target_sources(gec_diff PRIVATE ${PROTO_SRCS})
# target_include_directories(gec_diff PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

# ---- 单元测试 ----
option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ---- 安装 ----
install(TARGETS gec_diff ARCHIVE DESTINATION lib)
install(FILES src/gec_diff.h DESTINATION include/gec_diff)
