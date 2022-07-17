java_test(
    name = "candle_aggregator_test",
    srcs = ["CandleAggregatorTest.java"],
    deps = [
        "//src/main/java/com/verlumen/tradestar/candles:candle_aggregator",
        "//src/main/java/com/verlumen/tradestar/candles:candles_module",
        "@maven//:com_google_inject_guice",
        "@maven//:org_apache_beam_beam_sdks_java_core",
        "@maven//:org_apache_beam_beam_sdks_java_extensions_protobuf",
        "@maven//:org_apache_beam_beam_sdks_java_test_utils",
        "@tradestar_protos//:candles_java_proto",
        "@tradestar_protos//:trading_java_proto",
    ],
)
