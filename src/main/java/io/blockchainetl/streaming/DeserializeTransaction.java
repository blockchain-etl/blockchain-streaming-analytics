package io.blockchainetl.streaming;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DeserializeTransaction extends DoFn<PubsubMessage, EthereumTransaction> {
    private Logger LOG = LoggerFactory.getLogger(DeserializeTransaction.class);
    private transient ObjectMapper mapper;

    @Setup
    public void setup() {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        PubsubMessage msg = c.element();
        String jsonString = new String(msg.getPayload());

        try {
            EthereumTransaction tx = mapper.readValue(jsonString, EthereumTransaction.class);
            c.output(tx);
        } catch (IOException e) {
            LOG.error("Error parsing message: " + e.getMessage());
        }
    }
}
