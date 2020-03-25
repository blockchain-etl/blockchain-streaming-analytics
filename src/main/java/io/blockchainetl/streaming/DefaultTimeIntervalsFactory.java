package io.blockchainetl.streaming;

import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.Arrays;
import java.util.List;

public class DefaultTimeIntervalsFactory implements DefaultValueFactory<List<Integer>> {
    /**
     * Creates a default value for calculation time intervals.
     *
     * @param options The current pipeline options.
     * @return The default value to be used for the time intervals.
     */
    @Override
    public List<Integer> create(PipelineOptions options) {
        return Arrays.asList(1, 3, 5, 15, 30, 60);
    }
}
