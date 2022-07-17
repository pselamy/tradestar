package com.verlumen.tradestar.candles;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.verlumen.tradestar.protos.candles.Candle;
import com.verlumen.tradestar.protos.candles.Granularity;
import com.verlumen.tradestar.protos.trading.ExchangeTrade;
import org.apache.beam.sdk.values.PCollection;

public interface CandleAggregator {
  AggregateResult aggregate(AggregateParams params);

  interface CandleService {
    ImmutableSet<Candle> getCandles();
  }

  @AutoValue
  abstract class AggregateParams {
    static AggregateParams create(CandleService candleService, PCollection<ExchangeTrade> trades) {
      return new AutoValue_CandleAggregator_AggregateParams(candleService, trades);
    }

    @Memoized
    ImmutableSet<Candle> candles() {
      return candleService().getCandles();
    }

    abstract CandleService candleService();

    abstract PCollection<ExchangeTrade> trades();
  }

  @AutoValue
  abstract class AggregateResult {
    static AggregateResult create(ImmutableMap<Granularity, PCollection<Candle>> candles) {
      return new AutoValue_CandleAggregator_AggregateResult(candles);
    }

    public abstract ImmutableMap<Granularity, PCollection<Candle>> candles();
  }
}
