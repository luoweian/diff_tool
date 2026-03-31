cc_library(
    name = 'gec_diff',
    srcs = [
        'src/*.cpp',
    ],
    incs = ['src'],
    warning = 'all',
    deps = [
        "cpputil/databusclient:1.2.0@//cpputil/databusclient:databus",
    ],
    visibility = ['PUBLIC']
)

cc_test(
    name = 'gec_diff_test',
    srcs = [
        'tests/test_diff_value.cpp',
        'tests/test_gec_diff.cpp',
    ],
    deps = [':gec_diff'],
)
