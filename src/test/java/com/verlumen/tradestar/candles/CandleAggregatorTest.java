package com.verlumen.tradestar.candles;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.testing.junit.testparameterinjector.TestParameter;
import com.google.testing.junit.testparameterinjector.TestParameterInjector;
import com.verlumen.tradestar.candles.CandleAggregator.AggregateParams;
import com.verlumen.tradestar.candles.CandleAggregator.AggregateResult;
import com.verlumen.tradestar.protos.candles.Candle;
import com.verlumen.tradestar.protos.candles.Granularity;
import com.verlumen.tradestar.protos.trading.ExchangeTrade;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Comparator;
import java.util.EnumSet;

import static com.google.common.truth.Truth.assertThat;

@RunWith(TestParameterInjector.class)
public class CandleAggregatorTest {
  private CandleAggregator aggregator;
  private Pipeline pipeline;
  private Sleeper sleeper;

  @Before
  public void setup() {
    this.aggregator =
        Guice.createInjector(CandlesModule.TestModule.create(sleeper))
            .getInstance(CandleAggregator.class);
    this.pipeline = TestPipeline.create();
    this.sleeper = FakeSleeper.create();
  }

  @Test
  public void aggregate_aggregatesCandles(
      @TestParameter AggregateAggregatesCandlesTestCase testCase) throws Exception {
    // Arrange
    PCollection<ExchangeTrade> trades =
        createPCollection(testCase.trades, ProtoCoder.of(ExchangeTrade.class));
    FakeCandleService candleService = FakeCandleService.create(testCase.candles);
    FakeTradeService tradeService = FakeTradeService.create(trades);
    AggregateParams params = AggregateParams.create(candleService, pipeline, tradeService);

    // Act
    AggregateResult actual = aggregator.aggregate(params);

    // Assert
    assertThat(actual.candles()).isNotNull();
    EnumSet.allOf(Granularity.class).stream()
        .filter(Constants.SUPPORTED_GRANULARITIES::contains)
        .forEach(granularity -> assertThatCandlesAreAggregated(testCase, actual, granularity));
  }

  private void assertThatCandlesAreAggregated(
      AggregateAggregatesCandlesTestCase testCase,
      AggregateResult actual,
      Granularity granularity) {
    PCollection<Candle> actualCandleCollection = actual.candles().get(granularity);
    ImmutableSet<Candle> expectedCandles =
        testCase.expected.getOrDefault(granularity, ImmutableSet.of());

    assertThat(actualCandleCollection).isNotNull();
    PAssert.that(actualCandleCollection)
        .satisfies(
            actualCandles -> {
              assertThat(actualCandles)
                  .isInStrictOrder(
                      Comparator.<Candle>comparingLong(candle -> candle.getStart().getSeconds()));
              return null;
            });
    PAssert.that(actualCandleCollection).containsInAnyOrder(expectedCandles);
  }

  private <T> PCollection<T> createPCollection(ImmutableSet<T> tList, Coder<T> tCoder) {
    return pipeline.apply(Create.of(tList).withCoder(tCoder));
  }

  @SuppressWarnings("unused")
  private enum AggregateAggregatesCandlesTestCase {
    NO_CANDLES_NO_TRADES(ImmutableSet.of(), ImmutableSet.of(), ImmutableMap.of());

    private final ImmutableSet<Candle> candles;
    private final ImmutableSet<ExchangeTrade> trades;
    private final ImmutableMap<Granularity, ImmutableSet<Candle>> expected;

    AggregateAggregatesCandlesTestCase(
        ImmutableSet<Candle> candles,
        ImmutableSet<ExchangeTrade> trades,
        ImmutableMap<Granularity, ImmutableSet<Candle>> expected) {
      this.candles = candles;
      this.trades = trades;
      this.expected = expected;
    }
  }

  @AutoValue
  abstract static class FakeCandleService implements CandleAggregator.CandleService {
    private static FakeCandleService create(ImmutableSet<Candle> candles) {
      return new AutoValue_CandleAggregatorTest_FakeCandleService(candles);
    }
  }

  @AutoValue
  abstract static class FakeSleeper implements Sleeper {
    private static FakeSleeper create() {
      return new AutoValue_CandleAggregatorTest_FakeSleeper();
    }

    @Override
    public void sleep(long millis) {}
  }

  @AutoValue
  abstract static class FakeTradeService implements CandleAggregator.TradeService {
    private static FakeTradeService create(PCollection<ExchangeTrade> trades) {
      return new AutoValue_CandleAggregatorTest_FakeTradeService(trades);
    }
  }
}
