package io.blockchainetl.streaming.candlestick.fns;

import io.blockchainetl.streaming.candlestick.domain.Candlestick;
import io.blockchainetl.streaming.candlestick.fns.CandlestickToJson;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class CandlestickToJsonTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(true);

    @Test
    public void processElement() {
        Candlestick candlestick = new Candlestick(
                Instant.ofEpochSecond(1234567890L),
                10L, 8L, 6L, 11L
        );

        PCollection<String> json = testPipeline
                .apply(Create.of(candlestick))
                .apply("To JSON", ParDo.of(new CandlestickToJson()));

        PAssert
                .that(json)
                .containsInAnyOrder("{\"timestamp\":1234567890000,\"open\":10,\"close\":8,\"low\":6,\"high\":11}");

        testPipeline.run();
    }
}
