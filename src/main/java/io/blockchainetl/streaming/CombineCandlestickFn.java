package io.blockchainetl.streaming;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.joda.time.Instant;

public class CombineCandlestickFn extends Combine.CombineFn<Candlestick, CombineCandlestickFn.Accum, Candlestick> {
    @DefaultCoder(AvroCoder.class)
    static class Accum {
        @Nullable
        Instant openTimestamp;
        @Nullable
        Instant closeTimestamp;
        @Nullable
        Long open;
        @Nullable
        Long close;
        @Nullable
        Long low;
        @Nullable
        Long high;

        Accum() {
        }

        Accum(Instant openTimestamp, Instant closeTimestamp, long open, long close, long low, long high) {
            this.openTimestamp = openTimestamp;
            this.closeTimestamp = closeTimestamp;
            this.open = open;
            this.close = close;
            this.low = low;
            this.high = high;
        }

        Accum merge(Accum input) {
            if (input.low != null) {
                if (low == null) {
                    low = input.low;
                } else {
                    low = Math.min(low, input.low);
                }
            }

            if (input.high != null) {
                if (high == null) {
                    high = input.high;
                } else {
                    high = Math.max(high, input.high);
                }
            }

            if (input.openTimestamp != null) {
                if (openTimestamp == null) {
                    openTimestamp = input.openTimestamp;
                    open = input.open;
                } else {
                    if (input.openTimestamp.compareTo(openTimestamp) < 0) {
                        openTimestamp = input.openTimestamp;
                        open = input.open;
                    }
                }
            }

            if (input.closeTimestamp != null) {
                if (closeTimestamp == null) {
                    closeTimestamp = input.closeTimestamp;
                    close = input.close;
                } else {
                    if (input.closeTimestamp.compareTo(closeTimestamp) > 0) {
                        closeTimestamp = input.closeTimestamp;
                        close = input.close;
                    }
                }
            }

            return this;
        }
    }

    @Override
    public Accum createAccumulator() {
        return new Accum();
    }

    @Override
    public Accum addInput(Accum accum, Candlestick input) {
        Accum inputAccum = new Accum(
                input.getTimestamp(),
                input.getTimestamp(),
                input.getOpen(),
                input.getClose(),
                input.getLow(),
                input.getHigh()
        );

        return accum.merge(inputAccum);
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = createAccumulator();
        for (Accum accum : accums) {
            merged.merge(accum);
        }

        return merged;
    }

    @Override
    public Candlestick extractOutput(Accum accum) {
        return new Candlestick(accum.closeTimestamp, accum.open, accum.close, accum.low, accum.high);
    }
}
