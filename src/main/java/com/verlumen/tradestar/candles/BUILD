package(default_visibility = ["//:__subpackages__"])

java_binary(
    name = "candle_aggregation_pipeline",
    srcs = ["CandleAggregationPipeline.java"],
    main_class = "com.verlumen.tradestar.CandleAggregationPipeline",
    deps = [
        "@maven//:org_apache_beam_beam_sdks_java_core",
    ],
)

java_library(
    name = "candle_aggregator",
    srcs = ["CandleAggregator.java"],
    deps = [
        "//:autovalue",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_beam_beam_sdks_java_core",
        "@tradestar_protos//:candles_java_proto",
        "@tradestar_protos//:trading_java_proto",
    ],
)

java_library(
    name = "candle_aggregator_impl",
    srcs = [
        "CandleAggregatorImpl.java",
    ],
    deps = [
        ":candle_aggregator",
        "//:autovalue",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_inject_guice",
        "@maven//:com_google_mug_mug",
        "@maven//:joda_time_joda_time",
        "@maven//:org_apache_beam_beam_sdks_java_core",
        "@maven//:org_apache_beam_beam_sdks_java_extensions_protobuf",
        "@maven//:org_knowm_xchange_xchange_core",
        "@maven//:org_ta4j_ta4j_core",
        "@tradestar_core//src/main/java/com/verlumen/tradestar/core/candles:granularity_spec",
        "@tradestar_protos//:candles_java_proto",
        "@tradestar_protos//:trading_java_proto",
    ],
)

java_library(
    name = "candles_module",
    srcs = ["CandlesModule.java"],
    deps = [
        ":candle_aggregator",
        ":candle_aggregator_impl",
        "@maven//:com_google_inject_guice",
    ],
)

java_library(
    name = "constants",
    srcs = ["Constants.java"],
    deps = [
        "@maven//:com_google_guava_guava",
    ],
)
