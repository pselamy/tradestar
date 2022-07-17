package com.verlumen.tradestar.candles;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.verlumen.tradestar.protos.candles.Candle;
import com.verlumen.tradestar.protos.candles.Granularity;
import com.verlumen.tradestar.protos.trading.ExchangeTrade;
import org.apache.beam.sdk.values.PCollection;

public interface CandleAggregator {
  AggregateResult aggregate(AggregateParams params);

  @AutoValue
  abstract class AggregateParams {
    abstract PCollection<Candle> candles();

    abstract PCollection<ExchangeTrade> trades();
  }

  @AutoValue
  abstract class AggregateResult {
    static AggregateResult create(PCollection<Candle> candles) {
      return new AutoValue_CandleAggregator_AggregateResult(candles);
    }

    public abstract ImmutableMap<Granularity, PCollection<Candle>> candles();
  }
}
