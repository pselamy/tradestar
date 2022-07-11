package com.verlumen.tradestar.candles;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.ta4j.core.num.DoubleNum;

import javax.inject.Inject;
import java.util.Arrays;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.function.Function.identity;
import static org.apache.beam.sdk.transforms.windowing.Window.into;


class CandleAggregatorImpl implements CandleAggregator {
    private static final Duration ONE_MINUTE = Duration.standardMinutes(1);
    private final ImmutableMap<GranularitySpec, Window<?>> WINDOWS =
            Arrays.stream(GranularitySpec.values())
                    .collect(ImmutableMap.toImmutableMap(
                            identity(),
                            granularitySpec -> into(FixedWindows.of(
                                    Duration.standardMinutes(granularitySpec.minutes())))
                    ));

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
                createCandles(trades, GranularitySpec.ONE_MINUTE);

        BiStream.<Granularity, Window<Candle>>from(windows())
                .mapToObj((granularity, window) ->
                        applyWindow(oneMinuteCandles, window)
                                .apply(ParDo.of(CandleFactoryFn
                                        .create(granularity))));

        ImmutableList<PCollection<Candle>> collect = BiStream.from(WINDOWS)
                .map((granularity, window) -> aggregate(trades, granularity,
                        window))
                .collect(toImmutableList());
        PCollectionList<Candle> candleCollections =
                PCollectionList.of(collect);
        return candleCollections.apply(Flatten.pCollections());
    }

    private PCollection<Candle> createCandles(PCollection<ExchangeTrade> trades, GranularitySpec granularitySpec) {
        return applyWindow(trades,
                window(Duration.standardMinutes(granularitySpec.minutes())))
                .apply(ParDo.of(CandleFactoryFn.create(granularitySpec.granularity())));
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
    abstract static class CandleFactoryFn
            extends DoFn<Iterable<ExchangeTrade>, Candle> {
        private static CandleFactoryFn create(Granularity granularity) {
            return new AutoValue_CandleAggregatorImpl_CandleFactoryFn(granularity);
        }

        abstract Granularity granularity();

        @ProcessElement
        public void processElement(ProcessContext c) {
            Iterable<ExchangeTrade> trades =
                    firstNonNull(c.element(), ImmutableList.of());

            Bar bar = BaseBar.builder().build();
            Candle.Builder candleBuilder = Candle.newBuilder();
            for (ExchangeTrade trade : trades) {
                if (candleBuilder.getStartBuilder().getSeconds() == 0) {
                    long timestampInSeconds = trade.getTimestamp().getSeconds();
                    long secondsPastStart =
                            timestampInSeconds % granularity().seconds();
                    candleBuilder.getStartBuilder()
                            .setSeconds(timestampInSeconds - secondsPastStart);
                }

                bar.addTrade(DoubleNum.valueOf(trade.getPrice()),
                        DoubleNum.valueOf(trade.getVolume()));
            }

            c.output(candleBuilder
                    .setGranularity(granularity())
                    .setOpen(bar.getOpenPrice().doubleValue())
                    .setClose(bar.getClosePrice().doubleValue())
                    .setHigh(bar.getHighPrice().doubleValue())
                    .setLow(bar.getLowPrice().doubleValue())
                    .setVolume(bar.getVolume().doubleValue())
                    .build());
        }
    }
}
