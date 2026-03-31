cc_library(
    name = 'gec_diff',
    srcs = [
        'src/*.cpp',
    ],
    hdrs = [
        'src/*.h',
    ],
    incs = ['src'],
    warning = 'all',
    visibility = ['//...'],
)

cc_test(
    name = 'gec_diff_test',
    srcs = [
        'tests/test_diff_value.cpp',
        'tests/test_gec_diff.cpp',
    ],
    deps = [':gec_diff'],
)
