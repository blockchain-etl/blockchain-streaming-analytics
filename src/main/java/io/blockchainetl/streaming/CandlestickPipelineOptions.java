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

    @Description("List of time intervals defined in minutes to calculate candlestick aggregates")
    @Default.InstanceFactory(DefaultTimeIntervalsFactory.class)
    List<Integer> getTimeIntervals();

    void setTimeIntervals(List<Integer> value);

    @Description("Triggering interval. How often to emit updated values.")
    @Default.Integer(30)
    Integer getTriggeringInterval();

    void setTriggeringInterval(Integer value);
}
