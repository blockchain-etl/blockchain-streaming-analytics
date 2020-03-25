package io.blockchainetl.streaming;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

@DefaultCoder(AvroCoder.class)
public class Candlestick {
    Candlestick() {
    }

    Candlestick(Instant timestamp, Long open, Long close, Long low, Long high) {
        this.timestamp = timestamp;
        this.open = open;
        this.close = close;
        this.low = low;
        this.high = high;
    }

    private Instant timestamp;

    private Long open;

    private Long close;

    private Long low;

    private Long high;

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Long getOpen() {
        return open;
    }

    public void setOpen(long open) {
        this.open = open;
    }

    public Long getClose() {
        return close;
    }

    public void setClose(long close) {
        this.close = close;
    }

    public Long getLow() {
        return low;
    }

    public void setLow(long low) {
        this.low = low;
    }

    public Long getHigh() {
        return high;
    }

    public void setHigh(long high) {
        this.high = high;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Candlestick that = (Candlestick) o;
        return Objects.equal(timestamp, that.timestamp) &&
                Objects.equal(open, that.open) &&
                Objects.equal(close, that.close) &&
                Objects.equal(low, that.low) &&
                Objects.equal(high, that.high);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(timestamp, open, close, low, high);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("timestamp", timestamp)
                .add("open", open)
                .add("close", close)
                .add("low", low)
                .add("high", high)
                .toString();
    }
}
