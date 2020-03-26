package io.blockchainetl.streaming;


import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

import java.util.List;

public interface CandlestickPipelineOptions extends PipelineOptions, GcpOptions {

    @Description("Input type")
    @Default.String("ethereum")
    String getInputType();

    void setInputType(String value);

    @Description("GCP PubSub topic name to subscribe and read messages from")
    @Validation.Required
    String getInputDataTopicOrSubscription();

    void setInputDataTopicOrSubscription(String value);

    @Description("Prefix for GCP PubSub topics to write results in")
    @Default.String("ethereum_candles_")
    String getOutputTopicsPrefix();

    void setOutputTopicsPrefix(String value);

    // TODO: consider defining durations in ISO-8601 notation e.g. - PT1M, PT3M etc.
    @Description("List of windows' durations defined in seconds to calculate candlestick aggregates")
    @Default.InstanceFactory(DefaultAggregationWindowsFactory.class)
    List<Integer> getAggregationWindowsInSeconds();

    void setAggregationWindowsInSeconds(List<Integer> value);

    @Description("Triggering interval. How often to emit updated values.")
    @Default.Integer(30)
    Integer getTriggeringInterval();

    void setTriggeringInterval(Integer value);
}
