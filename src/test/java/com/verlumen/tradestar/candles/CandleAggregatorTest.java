package com.verlumen.tradestar.candles;

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
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Comparator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

@RunWith(TestParameterInjector.class)
public class CandleAggregatorTest {
  private CandleAggregator aggregator;
  private Pipeline pipeline;

  @Before
  public void setup() {
    this.aggregator = Guice.createInjector(new CandlesModule()).getInstance(CandleAggregator.class);
    this.pipeline = TestPipeline.create();
  }

  @Test
  public void aggregate_aggregatesCandles(
      @TestParameter AggregateAggregatesCandlesTestCase testCase) {
    // Arrange
    PCollection<ExchangeTrade> trades =
        createPCollection(testCase.trades, ProtoCoder.of(ExchangeTrade.class));
    AggregateParams params = AggregateParams.create(testCase.candles, trades);

    // Act
    AggregateResult actual = aggregator.aggregate(params);

    // Assert
    assertThat(actual.candles()).isNotNull();
    testCase.expected.forEach(
        (granularity, expectedCandles) ->
            assertThatCandlesAreAggregated(actual.candles().get(granularity), expectedCandles));
  }

  @Test
  public void aggregate_throwsExceptions(@TestParameter AggregateThrowsExceptionTestCase testCase) {
    // Arrange
    PCollection<ExchangeTrade> trades =
        createPCollection(testCase.trades, ProtoCoder.of(ExchangeTrade.class));
    AggregateParams params = AggregateParams.create(testCase.candles, trades);

    // Act / Assert
    assertThrows(testCase.expectedException.getClass(), () -> aggregator.aggregate(params));
  }

  private <T> PCollection<T> createPCollection(ImmutableSet<T> tList, Coder<T> tCoder) {
    return pipeline.apply(Create.of(tList).withCoder(tCoder));
  }

  private void assertThatCandlesAreAggregated(
      PCollection<Candle> actual, ImmutableSet<Candle> expected) {
    assertThat(actual).isNotNull();
    PAssert.that(actual)
        .satisfies(
            actualCandles -> {
              assertThat(actualCandles)
                  .isInStrictOrder(
                      Comparator.<Candle>comparingLong(candle -> candle.getStart().getSeconds()));
              return null;
            });
    PAssert.that(actual).containsInAnyOrder(expected);
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

  private enum AggregateThrowsExceptionTestCase {
    ;

    private final ImmutableSet<Candle> candles;
    private final ImmutableSet<ExchangeTrade> trades;
    private final Exception expectedException;

    AggregateThrowsExceptionTestCase(
        ImmutableSet<Candle> candles,
        ImmutableSet<ExchangeTrade> trades,
        Exception expectedException) {
      this.candles = candles;
      this.trades = trades;
      this.expectedException = expectedException;
    }
  }
}
