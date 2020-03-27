package io.blockchainetl.streaming.candlestick.fns;

import io.blockchainetl.streaming.candlestick.domain.Candlestick;
import io.blockchainetl.streaming.candlestick.transforms.TransactionToCandlestick;
import io.blockchainetl.streaming.candlestick.domain.EthereumTransaction;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;


public class CombineCandlestickFnTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(true);

    @Test
    public void combineSameBlock() {
        PCollection<Candlestick> candlesticks = testPipeline
                .apply(Create.of(
                        // open
                        getTransaction(10, 1571789348000L, 1),
                        // high
                        getTransaction(100, 1571789348000L, 2),
                        // low
                        getTransaction(1, 1571789348000L, 3),
                        // close
                        getTransaction(10, 1571789348000L, 4)
                ))
                .apply(new TransactionToCandlestick())
                .apply("Combine", Combine.globally(new CombineCandlestickFn()).withoutDefaults());

        PAssert
                .that(candlesticks)
                .containsInAnyOrder(new Candlestick(
                        Instant.ofEpochMilli(1571789348000L),
                        10L, 10L, 1L, 100L
                ));

        testPipeline.run();
    }

    @Test
    public void combineDifferentBlocks() {
        PCollection<Candlestick> candlesticks = testPipeline
                .apply(Create.of(
                        // open & high
                        getTransaction(200, 1571789344000L, 1),
                        getTransaction(100, 1571789348000L, 1),
                        // low
                        getTransaction(1, 1571789348000L, 2),
                        getTransaction(10, 1571789348000L, 3),
                        // close
                        getTransaction(5, 1571789349000L, 1)
                ))
                .apply(new TransactionToCandlestick())
                .apply("Combine", Combine.globally(new CombineCandlestickFn()).withoutDefaults());

        PAssert
                .that(candlesticks)
                .containsInAnyOrder(new Candlestick(
                        Instant.ofEpochMilli(1571789349000L),
                        200L, 5L, 1L, 200L
                ));

        testPipeline.run();
    }

    @Test
    public void combineSingleTransaction() {
        PCollection<Candlestick> candlesticks = testPipeline
                .apply(Create.of(
                        // open & close & low & high
                        getTransaction(1, 1571789348000L, 2)
                ))
                .apply(new TransactionToCandlestick())
                .apply("Combine", Combine.globally(new CombineCandlestickFn()).withoutDefaults());

        PAssert
                .that(candlesticks)
                .containsInAnyOrder(new Candlestick(
                        Instant.ofEpochMilli(1571789348000L),
                        1L, 1L, 1L, 1L
                ));

        testPipeline.run();
    }

    EthereumTransaction getTransaction(long price, long blockTimestamp, long index) {
        EthereumTransaction tx = new EthereumTransaction();
        tx.setTransactionIndex(index);
        tx.setGasPrice(price);
        tx.setBlockTimestamp(blockTimestamp);

        return tx;
    }
}
