package io.blockchainetl.streaming.candlestick.fns;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import io.blockchainetl.streaming.candlestick.domain.Candlestick;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CandlestickToJson extends DoFn<Candlestick, String> {
    private Logger LOG = LoggerFactory.getLogger(DeserializeTransaction.class);
    private transient ObjectMapper mapper;

    @Setup
    public void setup() {
        mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Candlestick candlestick = c.element();
        try {
            c.output(mapper.writeValueAsString(candlestick));
        } catch (JsonProcessingException e) {
            LOG.error("Error serializing candlestick: " + e.getMessage());
        }
    }
}
