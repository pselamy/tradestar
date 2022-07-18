package com.verlumen.tradestar.candles;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import com.google.mu.util.stream.BiStream;
import com.verlumen.tradestar.core.candles.GranularitySpec;
import com.verlumen.tradestar.protos.candles.Candle;
import com.verlumen.tradestar.protos.candles.Granularity;
import com.verlumen.tradestar.protos.trading.ExchangeTrade;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.ta4j.core.Bar;
import org.ta4j.core.BaseBar;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.function.Function.identity;
import static org.ta4j.core.num.DoubleNum.valueOf;

class CandleAggregatorImpl implements CandleAggregator {
  private static final Duration ONE_SECOND = Duration.standardSeconds(1);
  private static final Duration ONE_MINUTE = Duration.standardMinutes(1);
  private static final ImmutableMap<Granularity, Window<Candle>> WINDOWS =
      EnumSet.allOf(Granularity.class).stream()
          .filter(Constants.SUPPORTED_GRANULARITIES::contains)
          .collect(
              ImmutableMap.toImmutableMap(
                  identity(),
                  granularity -> window(Duration.standardMinutes(getMinutes(granularity)))));

  private final Clock clock;
  private final Sleeper sleeper;

  @Inject
  CandleAggregatorImpl(Clock clock, Sleeper sleeper) {
    this.clock = clock;
    this.sleeper = sleeper;
  }

  private static long getMinutes(Granularity granularity) {
    return GranularitySpec.fromGranularity(granularity).minutes();
  }

  private static <T> Window<T> window(Duration duration) {
    return Window.into(FixedWindows.of(duration));
  }

  @Override
  public AggregateResult aggregate(AggregateParams params) throws InterruptedException {
    StartTimeCalculator startTimeCalculator = StartTimeCalculator.create(clock);
    HistoricalOneMinuteCandleAggregator historicalOneMinuteCandleAggregator =
        HistoricalOneMinuteCandleAggregator.create(
            params.candleService(), clock, params.pipeline(), sleeper, startTimeCalculator);
    PCollection<Candle> historicalOneMinuteCandles =
        historicalOneMinuteCandleAggregator.aggregate();

    PCollection<Candle> liveOneMinuteCandles =
        applyWindow(params.trades(), window(ONE_MINUTE), new TradeTimestampFn())
            .apply(ParDo.of(OneMinuteCandleFn.create(startTimeCalculator)))
            .apply(Wait.on(historicalOneMinuteCandles));

    return AggregateResult.create(
        BiStream.from(WINDOWS)
            .mapValues(
                (granularity, window) ->
                    applyWindow(liveOneMinuteCandles, window, new CandleTimestampFn())
                        .apply(ParDo.of(CandleAggregationFn.create(granularity))))
            .toMap());
  }

  private <T> PCollection<Iterable<T>> applyWindow(
      PCollection<T> tCollection,
      Window<T> window,
      SerializableFunction<T, org.joda.time.Instant> timestampFn) {
    return tCollection
        .apply(WithTimestamps.of(timestampFn))
        .apply(window)
        .apply(WithKeys.of(1))
        .apply(GroupByKey.create())
        .apply(Values.create());
  }

  @AutoValue
  abstract static class CandleAggregationFn extends DoFn<Iterable<Candle>, Candle> {
    private static CandleAggregationFn create(Granularity granularity) {
      return new AutoValue_CandleAggregatorImpl_CandleAggregationFn(granularity);
    }

    abstract Granularity granularity();

    @ProcessElement
    public void processElement(ProcessContext c) {
      // One Minute Candles
      ImmutableList<Candle> candles = getCandles(c);
      GranularitySpec granularitySpec = GranularitySpec.fromGranularity(granularity());
      int minutes = (int) granularitySpec.minutes();
      if (candles.size() != minutes) {
        return;
      }

      checkState(
          candles.stream()
              .allMatch(candle -> candle.getGranularity().equals(Granularity.ONE_MINUTE)));

      c.output(
          Candle.newBuilder()
              .setOpen(candles.get(0).getOpen())
              .setHigh(
                  candles.stream()
                      .mapToDouble(Candle::getHigh)
                      .max()
                      .orElseThrow(IllegalStateException::new))
              .setLow(
                  candles.stream()
                      .mapToDouble(Candle::getLow)
                      .min()
                      .orElseThrow(IllegalStateException::new))
              .setClose(candles.get(minutes - 1).getClose())
              .setVolume(candles.stream().mapToDouble(Candle::getVolume).sum())
              .setGranularity(granularity())
              .build());
    }

    private ImmutableList<Candle> getCandles(DoFn<Iterable<Candle>, Candle>.ProcessContext c) {
      return ImmutableList.copyOf(firstNonNull(c.element(), ImmutableList.of()));
    }
  }

  @AutoValue
  abstract static class HistoricalOneMinuteCandleAggregator {
    private static HistoricalOneMinuteCandleAggregator create(
        CandleService candleService,
        Clock clock,
        Pipeline pipeline,
        Sleeper sleeper,
        StartTimeCalculator startTimeCalculator) {
      return new AutoValue_CandleAggregatorImpl_HistoricalOneMinuteCandleAggregator(
          candleService, clock, pipeline, ONE_SECOND, sleeper, startTimeCalculator);
    }

    abstract CandleService candleService();

    abstract Clock clock();

    abstract Pipeline pipeline();

    abstract Duration sleepDuration();

    abstract Sleeper sleeper();

    abstract StartTimeCalculator startTimeCalculator();

    PCollection<Candle> aggregate() throws InterruptedException {
      Instant startTime = startTimeCalculator().calculateProcessingStartTime();
      while (clock().instant().isBefore(startTime)) {
        sleeper().sleep(sleepDuration().getMillis());
      }

      ImmutableSet<Candle> candles = candleService().getCandles();
      return pipeline().apply(Create.of(candles).withCoder(ProtoCoder.of(Candle.class)));
    }
  }

  @AutoValue
  abstract static class OneMinuteCandleFn extends DoFn<Iterable<ExchangeTrade>, Candle> {
    private static OneMinuteCandleFn create(StartTimeCalculator startTimeCalculator) {
      return new AutoValue_CandleAggregatorImpl_OneMinuteCandleFn(startTimeCalculator);
    }

    private static Instant getTradeInstant(ExchangeTrade trade) {
      return Instant.ofEpochSecond(trade.getTimestamp().getSeconds());
    }

    abstract StartTimeCalculator startTimeCalculator();

    @ProcessElement
    public void processElement(ProcessContext c) {
      Instant processingStartTime = startTimeCalculator().calculateProcessingStartTime();
      ImmutableList<ExchangeTrade> trades =
          Optional.ofNullable(c.element())
              .map(Streams::stream)
              .orElseGet(Stream::empty)
              .filter(trade -> getTradeInstant(trade).isAfter(processingStartTime))
              .sorted(Comparator.comparing(OneMinuteCandleFn::getTradeInstant))
              .collect(ImmutableList.toImmutableList());
      if (trades.isEmpty()) {
        return;
      }

      Candle.Builder candleBuilder = Candle.newBuilder().setGranularity(Granularity.ONE_MINUTE);

      candleBuilder.getStartBuilder().setSeconds(getStartTime(trades));

      Bar bar = BaseBar.builder().build();
      trades.forEach(trade -> bar.addTrade(valueOf(trade.getPrice()), valueOf(trade.getVolume())));

      c.output(
          Candle.newBuilder()
              .setGranularity(Granularity.ONE_MINUTE)
              .setOpen(bar.getOpenPrice().doubleValue())
              .setClose(bar.getClosePrice().doubleValue())
              .setHigh(bar.getHighPrice().doubleValue())
              .setLow(bar.getLowPrice().doubleValue())
              .setVolume(bar.getVolume().doubleValue())
              .build());
    }

    private long getStartTime(ImmutableList<ExchangeTrade> trades) {
      long firstTradeTime = trades.get(0).getTimestamp().getSeconds();
      long secondsPastStart = firstTradeTime % 60;
      return firstTradeTime - secondsPastStart;
    }
  }

  @AutoValue
  abstract static class StartTimeCalculator {
    private static StartTimeCalculator create(Clock clock) {
      return new AutoValue_CandleAggregatorImpl_StartTimeCalculator(clock);
    }

    abstract Clock clock();

    @Memoized
    Instant calculateProcessingStartTime() {
      Instant now = clock().instant();
      Instant currentMinute = now.truncatedTo(ChronoUnit.MINUTES);
      return currentMinute.plusSeconds(ONE_MINUTE.getStandardSeconds());
    }
  }

  private static class CandleTimestampFn
      implements SerializableFunction<Candle, org.joda.time.Instant> {
    @Override
    public org.joda.time.Instant apply(Candle candle) {
      return new org.joda.time.Instant(candle.getStart().getSeconds());
    }
  }

  private static class TradeTimestampFn
      implements SerializableFunction<ExchangeTrade, org.joda.time.Instant> {
    @Override
    public org.joda.time.Instant apply(ExchangeTrade trade) {
      return new org.joda.time.Instant(trade.getTimestamp().getSeconds());
    }
  }
}
