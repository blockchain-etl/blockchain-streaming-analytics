package io.blockchainetl.streaming.candlestick.transforms;

import io.blockchainetl.streaming.candlestick.domain.Candlestick;
import io.blockchainetl.streaming.candlestick.fns.CombineCandlestickFn;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

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
                )
                .apply("Set timestamp to end of the window", ParDo.of(new DoFn<Candlestick, Candlestick>() {
                    @ProcessElement
                    public void processElement(
                            @Element Candlestick input,
                            OutputReceiver<Candlestick> output
                    ) {
                        Instant timestamp = roundTimestampUp(input.getTimestamp(), duration);
                        Candlestick candlestick = new Candlestick(
                                timestamp,
                                input.getOpen(),
                                input.getClose(),
                                input.getLow(),
                                input.getHigh()
                        );

                        output.output(candlestick);
                    }
                }));
    }

    static Instant roundTimestampUp(Instant timestamp, Duration duration) {
        long timestampMs = timestamp.getMillis();
        long durationMS = duration.getMillis();

        return Instant.ofEpochMilli(timestampMs - Math.floorMod(timestampMs, durationMS) + durationMS);
    }
}
