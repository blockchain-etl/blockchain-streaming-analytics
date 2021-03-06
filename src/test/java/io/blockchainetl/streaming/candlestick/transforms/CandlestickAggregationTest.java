package io.blockchainetl.streaming.candlestick.transforms;

import io.blockchainetl.streaming.candlestick.domain.Candlestick;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;


public class CandlestickAggregationTest {

    private static final int AGGREGATION_SECONDS = 60;
    private static final int TRIGGERING_INTERVAL = 30;

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(true);

    @Test
    public void testAggregationAndTriggering() {
        Trigger trigger = Repeatedly.forever(AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(TRIGGERING_INTERVAL)));

        Candlestick candlestick = new Candlestick(
                Instant.ofEpochMilli(5000L),
                10L, 9L, 8L, 10L
        );

        Candlestick candlestickSecondPane = new Candlestick(
                Instant.ofEpochMilli(5000L + TRIGGERING_INTERVAL * 1000),
                9L, 7L, 7L, 9L
        );

        TestStream<Candlestick> candlesticks = TestStream.create(AvroCoder.of(Candlestick.class))
                .addElements(TimestampedValue.of(candlestick, candlestick.getTimestamp()))
                .advanceProcessingTime(Duration.standardSeconds(5 + TRIGGERING_INTERVAL))
                .addElements(TimestampedValue.of(candlestickSecondPane, candlestickSecondPane.getTimestamp()))
                .advanceWatermarkToInfinity();

        PCollection<Candlestick> candlestickAggregation = testPipeline
                .apply(candlesticks)
                .apply(new CandlestickAggregation(Duration.standardSeconds(AGGREGATION_SECONDS), trigger));

        IntervalWindow window = new IntervalWindow(Instant.EPOCH, Duration.standardSeconds(AGGREGATION_SECONDS));
        PAssert
                .that(candlestickAggregation)
                .inEarlyPane(window)
                .containsInAnyOrder(new Candlestick(
                        Instant.ofEpochMilli(AGGREGATION_SECONDS * 1000),
                        10L, 9L, 8L, 10L
                ))
                .inFinalPane(window)
                .containsInAnyOrder(new Candlestick(
                        Instant.ofEpochMilli(AGGREGATION_SECONDS*1000),
                        10L, 7L, 7L, 10L
                ))
        ;

        testPipeline.run();
    }

    @Test
    public void testRoundTimestampUp() {
        // setup
        Instant ts = Instant.parse("2020-04-22T20:14:39.000Z");

        // test
        Instant roundedTs = CandlestickAggregation.roundTimestampUp(ts, Duration.standardSeconds(60));

        // check
        Assert.assertEquals("2020-04-22T20:15:00.000Z", roundedTs.toString());
    }
}
