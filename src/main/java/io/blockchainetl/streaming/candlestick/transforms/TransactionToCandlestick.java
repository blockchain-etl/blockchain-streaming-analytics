package io.blockchainetl.streaming.candlestick.transforms;

import io.blockchainetl.streaming.candlestick.domain.Candlestick;
import io.blockchainetl.streaming.candlestick.domain.EthereumTransaction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;


public class TransactionToCandlestick extends PTransform<PCollection<EthereumTransaction>, PCollection<Candlestick>> {
    @Override
    public PCollection<Candlestick> expand(PCollection<EthereumTransaction> input) {
        return input
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))))
                .apply(ParDo.of(new TransactionToKV()))
                .apply(GroupByKey.<Long, EthereumTransaction>create())
                .apply(ParDo.of(new AggregateToCandlestick()));
    }

    /**
     * Assigns to clicks random integer between zero and shardsNumber
     */
    private static class TransactionToKV extends DoFn<EthereumTransaction, KV<Long, EthereumTransaction>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            EthereumTransaction tx = c.element();
            c.output(KV.of(tx.getBlockTimestamp(), tx));
        }
    }

    private static class AggregateToCandlestick extends DoFn<KV<Long, Iterable<EthereumTransaction>>, Candlestick> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Long timestamp = c.element().getKey();
            Iterable<EthereumTransaction> transactions = c.element().getValue();

            Long openTransactionIndex = null;
            Long closeTransactionIndex = null;
            Candlestick candlestick = new Candlestick();
            candlestick.setTimestamp(new Instant(timestamp));

            for (EthereumTransaction tx : transactions) {
                if (candlestick.getLow() == null) {
                    candlestick.setLow(tx.getGasPrice());
                } else {
                    candlestick.setLow(Math.min(candlestick.getLow(), tx.getGasPrice()));
                }

                if (candlestick.getHigh() == null) {
                    candlestick.setHigh(tx.getGasPrice());
                } else {
                    candlestick.setHigh(Math.max(candlestick.getHigh(), tx.getGasPrice()));
                }

                if (openTransactionIndex == null || tx.getTransactionIndex() < openTransactionIndex) {
                    openTransactionIndex = tx.getTransactionIndex();
                    candlestick.setOpen(tx.getGasPrice());
                }

                if (closeTransactionIndex == null || tx.getTransactionIndex() > closeTransactionIndex) {
                    closeTransactionIndex = tx.getTransactionIndex();
                    candlestick.setClose(tx.getGasPrice());
                }
            }

            c.output(candlestick);
        }
    }
}
