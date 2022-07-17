package com.verlumen.tradestar.candles;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.mu.util.stream.BiStream;
import com.verlumen.tradestar.core.candles.GranularitySpec;
import com.verlumen.tradestar.protos.candles.Candle;
import com.verlumen.tradestar.protos.candles.Granularity;
import com.verlumen.tradestar.protos.trading.ExchangeTrade;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.ta4j.core.Bar;
import org.ta4j.core.BaseBar;

import java.util.Arrays;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.ta4j.core.num.DoubleNum.valueOf;


class CandleAggregatorImpl implements CandleAggregator {
    @Inject
    CandleAggregatorImpl() {
    }

    private static <T> Window<T> window(Duration duration) {
        return Window.into(FixedWindows.of(duration));
    }

    private static <T> ImmutableMap<Granularity, Window<T>> windows() {
        return Arrays
                .stream(GranularitySpec.values())
                .collect(ImmutableMap.toImmutableMap(
                        GranularitySpec::granularity,
                        granularitySpec -> window(Duration
                                .standardMinutes(granularitySpec.minutes()))));
    }

    @Override
    public PCollection<Candle> aggregate(PCollection<ExchangeTrade> trades) {
        PCollection<Candle> oneMinuteCandles =
                applyWindow(trades,
                        window(Duration.standardMinutes(1)))
                        .apply(ParDo.of(OneMinuteCandleFn.create()));

        PCollectionList<Candle> candleCollections =
                PCollectionList.of(BiStream.<Granularity, Window<Candle>>from(windows())
                        .mapToObj((granularity, window) ->
                                applyWindow(oneMinuteCandles, window)
                                        .apply(ParDo.of(CandleAggregationFn.create(granularity))))
                        .collect(toImmutableList()));
        return candleCollections.apply(Flatten.pCollections());
    }

    private <T> PCollection<Iterable<T>> applyWindow(
            PCollection<T> tCollection, Window<T> window) {
        return tCollection
                .apply(window)
                .apply(WithKeys.of(1))
                .apply(GroupByKey.create())
                .apply(Values.create());
    }

    @AutoValue
    abstract static class OneMinuteCandleFn extends DoFn<Iterable<ExchangeTrade>, Candle> {
        private static OneMinuteCandleFn create() {
            return new AutoValue_CandleAggregatorImpl_OneMinuteCandleFn();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            ImmutableList<ExchangeTrade> trades = ImmutableList.copyOf(
                    firstNonNull(c.element(), ImmutableList.of()));
            Candle.Builder candleBuilder =
                    Candle.newBuilder().setGranularity(Granularity.ONE_MINUTE);

            candleBuilder.getStartBuilder().setSeconds(getStartTime(trades));

            Bar bar = BaseBar.builder().build();
            for (ExchangeTrade trade : trades) {
                if (candleBuilder.getStartBuilder().getSeconds() == 0) {
                    long timestampInSeconds = trade.getTimestamp().getSeconds();
                    long secondsPastStart = timestampInSeconds % 60;
                    candleBuilder.getStartBuilder()
                            .setSeconds(timestampInSeconds - secondsPastStart);
                }

                bar.addTrade(valueOf(trade.getPrice()), valueOf(trade.getVolume()));
            }

            c.output(Candle.newBuilder()
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
    abstract static class CandleAggregationFn
            extends DoFn<Iterable<Candle>, Candle> {
        private static CandleAggregationFn create(Granularity granularity) {
            return new AutoValue_CandleAggregatorImpl_CandleAggregationFn(granularity);
        }

        abstract Granularity granularity();

        @ProcessElement
        public void processElement(ProcessContext c) {
            // One Minute Candles
            ImmutableList<Candle> candles = ImmutableList.copyOf(
                    firstNonNull(c.element(), ImmutableList.of()));

            GranularitySpec granularitySpec = GranularitySpec.fromGranularity(granularity());
            int minutes = (int) granularitySpec.minutes();
            checkArgument(candles.size() == minutes);
            checkArgument(candles.stream()
                    .allMatch(candle -> candle.getGranularity()
                            .equals(Granularity.ONE_MINUTE)));
            Candle.Builder candleBuilder = Candle.newBuilder();
            double open = candles.get(0).getOpen();
            double high = candles.stream().mapToDouble(Candle::getHigh).max()
                    .orElseThrow(IllegalStateException::new);
            double low = candles.stream().mapToDouble(Candle::getLow).min()
                    .orElseThrow(IllegalStateException::new);
            double close = candles.get(minutes - 1).getClose();
            double volume = candles.stream().mapToDouble(Candle::getVolume).sum();

            c.output(candleBuilder
                    .setOpen(open)
                    .setClose(close)
                    .setHigh(high)
                    .setLow(low)
                    .setVolume(volume)
                    .setGranularity(granularity())
                    .build());
        }
    }
}
