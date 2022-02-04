#!/usr/bin/env bash
export CLIENT_PATH=$PWD
if [ -z ${KAFKA_EXT_PATH+x} ];
  then echo "KAFKA_EXT_PATH is unset" && exit;
  else echo Path set to "$KAFKA_EXT_PATH"
fi

cd "${KAFKA_EXT_PATH}" || exit
./gradlew build -x test -x integrationTest hiveExtensionZip
unzip build/hivemq-extension/hivemq-kafka-extension-4.8.0-SNAPSHOT.zip -d "$CLIENT_PATH"/data/extensions
cd "$CLIENT_PATH" || exit
echo "$PWD"
cp ./data/configurations/kafka-configuration.xml ./data/extensions/hivemq-kafka-extension