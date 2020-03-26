package io.blockchainetl.streaming;

import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.Arrays;
import java.util.List;

public class DefaultAggregationWindowsFactory implements DefaultValueFactory<List<Integer>> {
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
