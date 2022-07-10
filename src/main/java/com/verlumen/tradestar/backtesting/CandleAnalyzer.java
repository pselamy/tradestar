package com.verlumen.tradestar.backtesting;

import com.verlumen.tradestar.protos.candles.Candle;
import com.verlumen.tradestar.protos.strategies.TradeStrategyTestResult;
import org.apache.beam.sdk.values.PCollection;

public interface CandleAnalyzer {
    PCollection<TradeStrategyTestResult> analyze(PCollection<Candle> candle);
}
