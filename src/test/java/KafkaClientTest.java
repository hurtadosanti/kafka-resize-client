import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
        sendMessageOfSize(512);
    }

    @Test
    public void sendLargeByteMessage() throws ExecutionException, InterruptedException {
        sendMessageOfSize(2048 * 1024);
    }

    @Test
    public void receiveMessage() {
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(senderTopic));
        AtomicInteger counter = new AtomicInteger();
        while (counter.get() <10) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));

            if (records.count() > 0) {
                records.forEach(record -> {
                    counter.getAndIncrement();
                    String s = new String(record.value(), StandardCharsets.UTF_8);
                    System.out.println(record.key()+"-"+ record.value().length);
                });
            }
        }
        //consumer.close();
    }

    private void sendMessageOfSize(int size) throws ExecutionException, InterruptedException {
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
        byte[] payload = ("start - "+RandomStringUtils.randomAscii(size)+" - end").getBytes(StandardCharsets.UTF_8);
        AtomicBoolean sent = new AtomicBoolean(false);
        Future<RecordMetadata> future = null;
        try {
            future = sendMessage(producer, payload, sent);
        } catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.RecordTooLargeException) {
                RecordTooLargeException recordInfo = ((RecordTooLargeException) e.getCause());
                String message = recordInfo.getMessage();
                producer.close();
                properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(payload.length + 256));
                producer = new KafkaProducer<>(properties);
                alterTopic();
                future = sendMessage(producer, payload, sent);
            } else {
                log.error(e.getCause().getClass().getCanonicalName());
            }

        } catch (Exception e) {
            log.error(e.getMessage());
        }
        assert future != null;
        assertTrue(future.isDone());
        assertTrue(sent.get());
    }

    private Future<RecordMetadata> sendMessage(KafkaProducer<String, byte[]> producer, byte[] payload, AtomicBoolean sent) throws ExecutionException, InterruptedException {
        final ProducerRecord<String, byte[]> record = new ProducerRecord<>(senderTopic, "key", payload);
        Future<RecordMetadata> future = producer.send(record, (m, e) -> {
            if (e != null) {
                log.debug("Send failed for record {}", record, e);
            } else {
                System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                sent.set(true);
            }
        });
        future.get();
        return future;
    }

    @NotNull
    private Properties getProperties() throws UnknownHostException {
        Properties config = new Properties();
        config.put("client.id", InetAddress.getLocalHost().getHostName());
        config.put("bootstrap.servers", "localhost:19092");
        //config.put(ProducerConfig.ACKS_CONFIG, "all");

        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "01");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
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
