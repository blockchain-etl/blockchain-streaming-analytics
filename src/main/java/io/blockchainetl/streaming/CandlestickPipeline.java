package io.blockchainetl.streaming;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
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

        PCollection<Candlestick> candlestick = pipeline
                .apply("Reading PubSub", readFromPubSub)
                .apply("Deserialize JSON", ParDo.of(new DeserializeTransaction()))
                .apply(new TransactionToCandlestick());

        PCollection<Candlestick> candlestick1min = candlestick
                .apply(
                        "Fixed windows",
                        Window.into(
                                // TODO: add triggering
                                FixedWindows.of(Duration.standardMinutes(1))))
                .apply(
                        "Calculate statistic",
                        Combine.globally(new CombineCandlestickFn()).withoutDefaults()
                );

        PCollection<Candlestick> candlestick3min = candlestick1min
                .apply(
                        "Fixed windows",
                        Window.into(
                                FixedWindows.of(Duration.standardMinutes(3))))
                .apply(
                        "Calculate statistic",
                        Combine.globally(new CombineCandlestickFn()).withoutDefaults()
                );

        PCollection<Candlestick> candlestick5min = candlestick1min
                .apply(
                        "Fixed windows",
                        Window.into(
                                FixedWindows.of(Duration.standardMinutes(5))))
                .apply(
                        "Calculate statistic",
                        Combine.globally(new CombineCandlestickFn()).withoutDefaults()
                );

        PCollection<Candlestick> candlestick15min = candlestick1min
                .apply(
                        "Fixed windows",
                        Window.into(
                                FixedWindows.of(Duration.standardMinutes(15))))
                .apply(
                        "Calculate statistic",
                        Combine.globally(new CombineCandlestickFn()).withoutDefaults()
                );

        candlestick1min
                .apply(ParDo.of(new CandlestickToJson()))
                .apply(PubsubIO.writeStrings()
                        .to("projects/" + options.getProject() + "/topics/" + options.getOutputTopicsPrefix() + "1min"));

        candlestick3min
                .apply(ParDo.of(new CandlestickToJson()))
                .apply(PubsubIO.writeStrings()
                        .to("projects/" + options.getProject() + "/topics/" + options.getOutputTopicsPrefix() + "3min"));

        candlestick5min
                .apply(ParDo.of(new CandlestickToJson()))
                .apply(PubsubIO.writeStrings()
                        .to("projects/" + options.getProject() + "/topics/" + options.getOutputTopicsPrefix() + "5min"));

        candlestick15min
                .apply(ParDo.of(new CandlestickToJson()))
                .apply(PubsubIO.writeStrings()
                        .to("projects/" + options.getProject() + "/topics/" + options.getOutputTopicsPrefix() + "15min"));

        pipeline.run();
    }
}
