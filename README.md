```shell script
gcloud pubsub topics create ethereum_candles_1min
gcloud pubsub topics create ethereum_candles_3min
gcloud pubsub topics create ethereum_candles_5min
gcloud pubsub topics create ethereum_candles_15min
```


```shell script
export PROJECT=$(gcloud config get-value project 2> /dev/null)
mvn compile exec:java -Dexec.mainClass=io.blockchainetl.streaming.CandlestickPipeline \
 -Dexec.args="--output=counts --inputDataTopicOrSubscription=projects/$PROJECT/subscriptions/crypto_ethereum.transactions" -Pdirect-runner
```

```shell script
java -cp target/blockchain-streaming-analytics-bundled-0.1.jar io.blockchainetl.streaming.CandlestickPipeline --runner=org.apache.beam.runners.DirectRunner --project=$PROJECT --inputDataTopicOrSubscri
ption=projects/$PROJECT/subscriptions/crypto_ethereum.transactions
```