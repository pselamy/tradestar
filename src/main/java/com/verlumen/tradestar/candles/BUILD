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
