cc_library(
    name = 'gec_diff',
    srcs = [
        'src/diff_value.cpp',
        'src/async_writer.cpp',
        'src/gec_diff.cpp',
    ],
    hdrs = [
        'src/diff_types.h',
        'src/diff_value.h',
        'src/async_writer.h',
        'src/gec_diff.h',
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
