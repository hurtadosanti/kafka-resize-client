import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaClientTest {
    final String senderTopic = "test_sender";
    private static final Logger log = LoggerFactory.getLogger(KafkaClientTest.class);
    private final Properties properties;
    KafkaClientTest() throws UnknownHostException {
        properties = getProperties();
    }
    @Test
    public void sendMessage() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        final ProducerRecord<String, String> record = new ProducerRecord<>(senderTopic, "key", "value");
        producer.send(record, (m, e) -> {
            if (e != null){
                log.debug("Send failed for record {}", record, e);}
            else {
                System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
            }
        });

    }

    @NotNull
    private Properties getProperties() throws UnknownHostException {
        Properties config = new Properties();
        config.put("client.id", InetAddress.getLocalHost().getHostName());
        config.put("bootstrap.servers", "localhost:19092");
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
        return config;
    }

    @Test
    public void createTopic() {
        final NewTopic newTopic = new NewTopic(senderTopic, Optional.empty(), Optional.empty());
        try (final AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }
    @Test
    public void alterTopic() throws ExecutionException, InterruptedException {

        final AdminClient adminClient = AdminClient.create(properties);

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC,senderTopic);
        ConfigEntry entry = new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, String.valueOf(2048*2048));
        AlterConfigOp op = new AlterConfigOp(entry,AlterConfigOp.OpType.SET);

        Map<ConfigResource,Collection<AlterConfigOp>> configs = new HashMap<>(1);
        configs.put(configResource, List.of(op));
        adminClient.incrementalAlterConfigs(configs).all().get();
        //final NewTopic newTopic = new NewTopic(senderTopic, Optional.empty(), Optional.empty());


    }
}
