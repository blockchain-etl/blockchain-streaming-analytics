package io.blockchainetl.streaming.candlestick;


import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.*;

import java.util.Arrays;
import java.util.List;

public interface CandlestickPipelineOptions extends PipelineOptions, GcpOptions {

    class DefaultAggregationWindowsFactory implements DefaultValueFactory<List<Integer>> {
        /**
         * Creates a default value for calculation time intervals.
         *
         * @param options The current pipeline options.
         * @return The default value to be used for the time intervals in seconds.
         */
        @Override
        public List<Integer> create(PipelineOptions options) {
            // 1min, 3min, 5min, 15min
            return Arrays.asList(60, 180, 300, 900);
        }
    }

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
