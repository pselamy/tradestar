java_binary(
    name = "backtesting_pipeline",
    srcs = ["BacktestingPipeline.java"],
    main_class = "com.verlumen.tradestar.BacktestingPipeline",
    deps = [
        "@maven//:org_apache_beam_beam_sdks_java_core",
    ],
)

java_library(
    name = "candle_analyzer",
    srcs = ["CandleAnalyzer.java"],
    deps = [
        "@maven//:org_apache_beam_beam_sdks_java_core",
        "@tradestar_protos//:candles_java_proto",
        "@tradestar_protos//:strategies_java_proto",
    ],
)

java_library(
    name = "trade_strategy_test_result_writer",
    srcs = [
        "TradeStrategyTestResultWriter.java",
    ],
    deps = [
        "@maven//:org_apache_beam_beam_sdks_java_core",
        "@tradestar_protos//:strategies_java_proto",
    ],
)
