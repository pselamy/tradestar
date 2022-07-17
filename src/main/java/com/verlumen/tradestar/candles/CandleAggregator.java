package com.verlumen.tradestar.candles;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.verlumen.tradestar.protos.candles.Candle;
import com.verlumen.tradestar.protos.candles.Granularity;
import com.verlumen.tradestar.protos.trading.ExchangeTrade;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.Map;

public interface CandleAggregator {
  AggregateResult aggregate(AggregateParams params);

  interface CandleService extends Serializable {
    ImmutableSet<Candle> getCandles();
  }

  interface TradeService extends Serializable {
    PCollection<ExchangeTrade> getTrades();
  }

  @AutoValue
  abstract class AggregateParams {
    static AggregateParams create(CandleService candleService, TradeService tradeService) {
      return new AutoValue_CandleAggregator_AggregateParams(candleService, tradeService);
    }

    @Memoized
    ImmutableSet<Candle> candles() {
      return candleService().getCandles();
    }

    @Memoized
    PCollection<ExchangeTrade> trades() {
      return tradeService().getTrades();
    }

    abstract CandleService candleService();

    abstract TradeService tradeService();
  }

  @AutoValue
  abstract class AggregateResult {
    static AggregateResult create(Map<Granularity, PCollection<Candle>> candles) {
      return create(ImmutableMap.copyOf(candles));
    }

    static AggregateResult create(ImmutableMap<Granularity, PCollection<Candle>> candles) {
      return new AutoValue_CandleAggregator_AggregateResult(candles);
    }

    public abstract ImmutableMap<Granularity, PCollection<Candle>> candles();
  }
}
