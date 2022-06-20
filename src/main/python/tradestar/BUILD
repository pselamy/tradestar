load(
    "@pip_deps//:requirements.bzl",
    "requirement",
)

py_binary(
    name = "candle_aggregation_pipeline",
    srcs = ["candle_aggregation_pipeline.py"],
    deps = [
        requirement("apache-beam"),
        requirement("tiingo"),
    ],
)
