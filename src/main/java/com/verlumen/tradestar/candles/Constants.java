package com.verlumen.tradestar.candles;

import com.google.common.collect.ImmutableSet;
import com.verlumen.tradestar.protos.candles.Granularity;

public class Constants {
  private static final ImmutableSet<Granularity> UNSUPPORTED_GRANULARITIES =
      ImmutableSet.of(Granularity.UNRECOGNIZED, Granularity.UNSPECIFIED);
}
