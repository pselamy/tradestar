package com.verlumen.tradestar.candles;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.verlumen.tradestar.protos.candles.Candle;
import com.verlumen.tradestar.protos.trading.ExchangeTrade;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.EnumSet.allOf;

@RunWith(Parameterized.class)
public class CandleAggregatorTest {
    private final CandleAggregator aggregator;
    private final Pipeline pipeline;
    private final AggregateTestCase testCase;

    public CandleAggregatorTest(AggregateTestCase testCase) {
        this.aggregator = Guice.createInjector(new CandlesModule())
                .getInstance(CandleAggregator.class);
        this.pipeline = TestPipeline.create();
        this.testCase = testCase;
    }

    @Parameters(name = "{0}")
    public static Iterable<?> data() {
        return allOf(AggregateTestCase.class).stream()
                .map(testCase -> new Object[]{testCase})
                .collect(toImmutableList());
    }

    @Test
    public void test() {
        PCollection<ExchangeTrade> trades = pipeline
                .apply(Create.of(testCase.trades))
                .setCoder(ProtoCoder.of(ExchangeTrade.class));

        PCollection<Candle> actual = aggregator.aggregate(trades);

        PAssert.that(actual).containsInAnyOrder(testCase.expected);
    }

    private enum AggregateTestCase {
        NO_TRADES(ImmutableList.of(), ImmutableList.of());

        private final ImmutableList<ExchangeTrade> trades;
        private final ImmutableList<Candle> expected;

        AggregateTestCase(ImmutableList<ExchangeTrade> trades, ImmutableList<Candle> expected) {
            this.trades = trades;
            this.expected = expected;
        }
    }
}