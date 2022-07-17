package com.verlumen.tradestar.candles;

import com.google.auto.value.AutoValue;
import com.google.inject.AbstractModule;
import org.apache.beam.sdk.util.Sleeper;

public class CandlesModule {
  private static class BaseModule extends AbstractModule {
    @Override
    protected void configure() {
      bind(CandleAggregator.class).to(CandleAggregatorImpl.class);
    }
  }

  @AutoValue
  public static class ProdModule extends AbstractModule {
    @Override
    protected void configure() {
      bind(Sleeper.class).toInstance(Sleeper.DEFAULT);

      install(new BaseModule());
    }
  }

  @AutoValue
  abstract static class TestModule extends AbstractModule {
    static TestModule create(Sleeper sleeper) {
      return new AutoValue_CandlesModule_TestModule(sleeper);
    }

    abstract Sleeper sleeper();

    @Override
    protected void configure() {
      install(new BaseModule());
    }
  }
}
