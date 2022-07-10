package com.verlumen.tradestar.candles;

import com.verlumen.tradestar.protos.candles.Candle;
import com.verlumen.tradestar.protos.trading.ExchangeTrade;
import org.apache.beam.sdk.values.PCollection;

public interface CandleAggregator {
    PCollection<Candle> aggregate(PCollection<ExchangeTrade> trades);
}
