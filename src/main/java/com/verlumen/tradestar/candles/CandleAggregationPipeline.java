package com.verlumen.tradestar.candles;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class CandleAggregationPipeline {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.run().waitUntilFinish();
    }
}