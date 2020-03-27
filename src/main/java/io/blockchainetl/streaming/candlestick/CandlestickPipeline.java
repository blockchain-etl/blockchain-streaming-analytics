package io.blockchainetl.streaming.candlestick;

import com.google.pubsub.v1.ProjectTopicName;
import io.blockchainetl.streaming.candlestick.fns.DeserializeTransaction;
import io.blockchainetl.streaming.candlestick.domain.Candlestick;
import io.blockchainetl.streaming.candlestick.fns.CandlestickToJson;
import io.blockchainetl.streaming.candlestick.transforms.CandlestickAggregation;
import io.blockchainetl.streaming.candlestick.transforms.TransactionToCandlestick;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class CandlestickPipeline {

    public static void main(String[] args) {
        CandlestickPipelineOptions options =
                PipelineOptionsFactory
                        .fromArgs(args)
                        .withValidation()
                        .as(CandlestickPipelineOptions.class);

        if (!options.getInputType().equals("ethereum")) {
            throw new RuntimeException("Currently only 'ethereum' input type supported");
        }

        Pipeline pipeline = Pipeline.create(options);

        PCollection<Candlestick> candlestick1s = pipeline
                .apply("Reading PubSub", getPubSubReadIO(options.getInputDataTopicOrSubscription()))
                .apply("Deserialize JSON", ParDo.of(new DeserializeTransaction()))
                .apply(new TransactionToCandlestick());

        Trigger trigger = Repeatedly.forever(AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(options.getTriggeringInterval())));

        // TODO: validate aggregationWindowsInSeconds option
        for (Integer windowSeconds : options.getAggregationWindowsInSeconds()) {
            ProjectTopicName topicName = ProjectTopicName.of(
                    options.getProject(),
                    options.getOutputTopicsPrefix() + windowSeconds.toString()
            );

            // TODO: create topic if not exists
            // TODO: optimisation - use most appropriate aggregate for the next aggregation
            candlestick1s
                    .apply(
                            "Aggregation of " + windowSeconds + "s",
                            new CandlestickAggregation(Duration.standardSeconds(windowSeconds), trigger)
                    )
                    .apply(
                            "Convert " + windowSeconds + "s candlestick to JSON",
                            ParDo.of(new CandlestickToJson())
                    )
                    .apply(
                            "Publish to " + topicName.getTopic(),
                            PubsubIO.writeStrings().to(topicName.toString())
                    );
        }

        pipeline.run();
    }

    private static PubsubIO.Read<PubsubMessage> getPubSubReadIO(String inputDataTopicOrSubscription) {
        if (inputDataTopicOrSubscription.contains("/topics/")) {
            return PubsubIO
                    .readMessagesWithAttributes()
                    .fromTopic(inputDataTopicOrSubscription);
        }

        return PubsubIO
                .readMessagesWithAttributes()
                .fromSubscription(inputDataTopicOrSubscription);
    }
}
