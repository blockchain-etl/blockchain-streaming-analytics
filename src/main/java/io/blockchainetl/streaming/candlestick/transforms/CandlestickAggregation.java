package io.blockchainetl.streaming.candlestick.transforms;

import io.blockchainetl.streaming.candlestick.domain.Candlestick;
import io.blockchainetl.streaming.candlestick.fns.CombineCandlestickFn;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class CandlestickAggregation extends PTransform<PCollection<Candlestick>, PCollection<Candlestick>> {
    private Duration duration;
    private Trigger trigger;

    public CandlestickAggregation(Duration duration, Trigger trigger) {
        this.duration = duration;
        this.trigger = trigger;
    }

    @Override
    public PCollection<Candlestick> expand(PCollection<Candlestick> input) {

        return input
                .apply(
                        "Fixed windows",
                        Window.<Candlestick>into(
                                FixedWindows.of(duration))
                                .triggering(trigger)
                                .withAllowedLateness(Duration.ZERO)
                                .accumulatingFiredPanes()
                )
                .apply(
                        "Calculate statistic",
                        Combine.globally(new CombineCandlestickFn()).withoutDefaults()
                );
    }
}
