package com.verlumen.tradestar.candles;

import com.google.auto.value.AutoValue;
import com.google.inject.AbstractModule;
import org.apache.beam.sdk.util.Sleeper;

import java.time.Clock;

public class CandleAggregatorModule {
  static TestModule testModule(Clock clock, Sleeper sleeper) {
    return new AutoValue_CandleAggregatorModule_TestModule(clock, sleeper);
  }

  @AutoValue
  abstract static class BaseModule extends AbstractModule {
    private static BaseModule create() {
      return new AutoValue_CandleAggregatorModule_BaseModule();
    }

    @Override
    protected void configure() {
      bind(CandleAggregator.class).to(CandleAggregatorImpl.class);
    }
  }

  @AutoValue
  abstract static class ProdModule extends AbstractModule {
    @Override
    protected void configure() {
      bind(Sleeper.class).toInstance(Sleeper.DEFAULT);

      install(BaseModule.create());
    }
  }

  @AutoValue
  abstract static class TestModule extends AbstractModule {
    abstract Clock clock();

    abstract Sleeper sleeper();

    @Override
    protected void configure() {
      install(BaseModule.create());

      bind(Clock.class).toInstance(clock());
      bind(Sleeper.class).toInstance(sleeper());
    }
  }
}
