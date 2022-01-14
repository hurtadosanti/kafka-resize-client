import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaClientTest {
    final String senderTopic = "test";
    private static final Logger log = LoggerFactory.getLogger(KafkaClientTest.class);
    private final Properties properties;

    KafkaClientTest() throws UnknownHostException {
        properties = getProperties();
    }

    @Test
    public void sendByteMessage() throws ExecutionException, InterruptedException {
        SendMessageOfSize(512);
    }

    @Test
    public void sendLargeByteMessage() throws ExecutionException, InterruptedException {
        SendMessageOfSize(2048 * 1024);
    }

    private void SendMessageOfSize(int size) throws InterruptedException, ExecutionException {
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
        byte[] payload = RandomStringUtils.randomAscii(size).getBytes(StandardCharsets.UTF_8);
        final ProducerRecord<String, byte[]> record = new ProducerRecord<>(senderTopic, "key", payload);
        AtomicBoolean sent = new AtomicBoolean(false);
        Future<RecordMetadata> future = producer.send(record, (m, e) -> {
            if (e != null) {
                log.debug("Send failed for record {}", record, e);
            } else {
                System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                sent.set(true);
            }
        });
        try {
            future.get();
        } catch (java.util.concurrent.ExecutionException e) {
            if(e.getCause() instanceof org.apache.kafka.common.errors.RecordTooLargeException){
                RecordTooLargeException recordInfo = ((RecordTooLargeException) e.getCause());
                String message = recordInfo.getMessage();

            }else{
                log.error(e.getCause().getClass().getCanonicalName());
            }

        }catch (Exception e) {
            log.error(e.getMessage());
        }
        assertTrue(future.isDone());
        assertTrue(sent.get());
    }

    @NotNull
    private Properties getProperties() throws UnknownHostException {
        Properties config = new Properties();
        config.put("client.id", InetAddress.getLocalHost().getHostName());
        config.put("bootstrap.servers", "localhost:19092");
        //config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
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

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, senderTopic);
        ConfigEntry entry = new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, String.valueOf(2048 * 2048));
        AlterConfigOp op = new AlterConfigOp(entry, AlterConfigOp.OpType.SET);

        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>(1);
        configs.put(configResource, List.of(op));
        KafkaFuture<Void> alterFuture = adminClient.incrementalAlterConfigs(configs).all();
        //final NewTopic newTopic = new NewTopic(senderTopic, Optional.empty(), Optional.empty());
        alterFuture.get();
        assertTrue(alterFuture.isDone());

    }
}
