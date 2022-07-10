package com.verlumen.tradestar.candles;

import com.verlumen.tradestar.protos.candles.Candle;
import com.verlumen.tradestar.protos.trading.ExchangeTrade;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

class CandleAggregatorImpl implements CandleAggregator {
    @Override
    public PCollection<Candle> aggregate(PCollection<ExchangeTrade> trades) {
        return trades
                .apply(Create.empty(ProtoCoder.of(ExchangeTrade.class)));
    }
}
