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
        ":candle_aggregation_pipeline",
        "//:autovalue",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_inject_guice",
        "@maven//:com_google_mug_mug",
        "@maven//:joda_time_joda_time",
        "@maven//:org_apache_beam_beam_sdks_java_core",
        "@maven//:org_apache_beam_beam_sdks_java_extensions_protobuf",
        "@tradestar_core//src/main/java/com/verlumen/tradestar/core/candles:granularity_spec",
        "@tradestar_protos//:candles_java_proto",
        "@tradestar_protos//:trading_java_proto",
    ],
)
