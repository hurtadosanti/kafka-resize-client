# hivemq-client-examples

Commands to run the samples

```shell
brew install kafka

docker-compose up -d

kafka-console-producer --broker-list localhost:19092 --topic test

kafka-console-consumer --bootstrap-server localhost:19092 --topic test --from-beginning

kafka-topics --bootstrap-server localhost:19092 --topic test --describe
```