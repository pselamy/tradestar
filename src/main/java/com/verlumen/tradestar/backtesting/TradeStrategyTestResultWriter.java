package com.verlumen.tradestar.backtesting;

import com.verlumen.tradestar.protos.strategies.TradeStrategyTestResult;

public interface TradeStrategyTestResultWriter {
    void write(TradeStrategyTestResult result);
}
