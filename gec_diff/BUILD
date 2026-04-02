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
        'tests/*.cpp',
    ],
    deps = [
        ':gec_diff',
        "cpp3rdlib/gtest:1.10.0@//cpp3rdlib/gtest:gtest",
        "cpputil/databusclient:1.2.0@//cpputil/databusclient:databus",
        'cpp3rdlib/boost:1.72.0@//cpp3rdlib/boost:boost',
    ]
)
