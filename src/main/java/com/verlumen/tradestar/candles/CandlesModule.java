package com.verlumen.tradestar.candles;

import com.google.inject.AbstractModule;

public class CandlesModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(CandleAggregator.class).to(CandleAggregatorImpl.class);
    }
}
