# Blockchain Streaming Analytics

## Create output topics manually
Where:
-  "ethereum_candles_" - topic prefix, default value of the `--outputTopicsPrefix` pipeline option;
- "60", "180", "300", "900" - aggregation windows in seconds, default value of the `--aggregationWindowsInSeconds` pipeline options.
  
```shell script
gcloud pubsub topics create ethereum_candles_60
gcloud pubsub topics create ethereum_candles_180
gcloud pubsub topics create ethereum_candles_300
gcloud pubsub topics create ethereum_candles_900
```

## Build pipeline

```shell script
mvn clean package
```

## Start pipeline locally
```shell script
export PROJECT=$(gcloud config get-value project 2> /dev/null)
java -cp target/blockchain-streaming-analytics-bundled-0.1.jar io.blockchainetl.streaming.CandlestickPipeline \
  --runner=DirectRunner \
  --project=$PROJECT \
  --inputDataTopicOrSubscription=projects/$PROJECT/subscriptions/crypto_ethereum.transactions
```

## Start pipeline in GCP with Dataflow runner
```shell script
export PROJECT=$(gcloud config get-value project 2> /dev/null)
java -cp target/blockchain-streaming-analytics-bundled-0.1.jar io.blockchainetl.streaming.CandlestickPipeline \
  --runner=org.apache.beam.runners.dataflow.DataflowRunner \
  --project=$PROJECT \
  --inputDataTopicOrSubscription=projects/$PROJECT/subscriptions/crypto_ethereum.transactions
```