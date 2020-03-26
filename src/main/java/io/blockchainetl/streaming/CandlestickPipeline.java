package io.blockchainetl.streaming;

import com.google.pubsub.v1.ProjectTopicName;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
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

        PubsubIO.Read<PubsubMessage> readFromPubSub;
        if (options.getInputDataTopicOrSubscription().contains("/topics/")) {
            readFromPubSub = PubsubIO
                    .readMessagesWithAttributes()
                    .fromTopic(options.getInputDataTopicOrSubscription());
        } else {
            readFromPubSub = PubsubIO
                    .readMessagesWithAttributes()
                    .fromSubscription(options.getInputDataTopicOrSubscription());
        }

        PCollection<Candlestick> candlestick1s = pipeline
                .apply("Reading PubSub", readFromPubSub)
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
            PCollection<Candlestick> candlestick = addCandlestickAggregate(
                    candlestick1s,
                    trigger,
                    topicName.toString(),
                    Duration.standardSeconds(windowSeconds)
            );
        }

        pipeline.run();
    }

    private static PCollection<Candlestick> addCandlestickAggregate(
            PCollection<Candlestick> candlestick,
            Trigger trigger,
            String topicName,
            Duration duration
    ) {
        PCollection<Candlestick> candlestickAggregation = candlestick
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
                );

        // TODO: instead of closeTimestamp use window end as Candlestick timestamp
        candlestickAggregation
                .apply(ParDo.of(new CandlestickToJson()))
                .apply(PubsubIO.writeStrings().to(topicName));

        return candlestickAggregation;
    }
}
