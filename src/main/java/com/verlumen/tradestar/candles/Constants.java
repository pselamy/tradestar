package com.verlumen.tradestar.candles;

import com.google.common.collect.ImmutableSet;
import com.verlumen.tradestar.protos.candles.Granularity;

import java.util.EnumSet;

public class Constants {
  private static final EnumSet<Granularity> UNSUPPORTED_GRANULARITIES =
      EnumSet.of(Granularity.UNRECOGNIZED, Granularity.UNSPECIFIED);
  static final ImmutableSet<Granularity> SUPPORTED_GRANULARITIES =
      ImmutableSet.copyOf(EnumSet.complementOf(UNSUPPORTED_GRANULARITIES));
}
