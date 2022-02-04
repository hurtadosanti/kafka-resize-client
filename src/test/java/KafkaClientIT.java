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
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaClientIT {
    final String senderTopic = "test";
    final int messageSize = 1024 * 1024 * 124;
    private static final Logger log = LoggerFactory.getLogger(KafkaClientIT.class);
    private final Properties properties;

    KafkaClientIT() throws UnknownHostException {
        properties = getProperties();
    }

    @Test
    public void sendByteMessage() throws ExecutionException, InterruptedException {
        sendMessageOfSize(512);
    }

    @Test
    public void sendLargeByteMessage() throws ExecutionException, InterruptedException {
        sendMessageOfSize(messageSize);
    }

    @Test
    public void receiveMessage() {
        Properties props = properties;
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(messageSize));
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(messageSize));
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(senderTopic));
        AtomicInteger counter = new AtomicInteger();
        while (counter.get() < 10) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));

            if (records.count() > 0) {
                records.forEach(record -> {
                    counter.getAndIncrement();
                    //String s = new String(record.value(), StandardCharsets.UTF_8);
                    System.out.println(record.key() + "-" + record.value().length);
                });
            }
        }
        //consumer.close();
    }

    private ProducerRecord<String, byte[]> getRecord(int size) {
        String largeString = RandomStringUtils.randomAscii(size / 4);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(largeString.getBytes(StandardCharsets.UTF_8));
        buffer.put(largeString.getBytes(StandardCharsets.UTF_8));
        buffer.put(largeString.getBytes(StandardCharsets.UTF_8));
        buffer.put(largeString.getBytes(StandardCharsets.UTF_8));
        return new ProducerRecord<>(senderTopic, "key", buffer.array());
    }

    private void sendMessageOfSize(int size) throws ExecutionException, InterruptedException {
        System.out.println("Message Size to be send:" + size);
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
        AtomicBoolean sent = new AtomicBoolean(false);
        ProducerRecord<String, byte[]> record = getRecord(size);
        int sendMessageSize = size + 128;
        Future<RecordMetadata> future = null;
        try {
            future = sendMessage(producer, record, sent);
        } catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.RecordTooLargeException) {
                RecordTooLargeException recordInfo = ((RecordTooLargeException) e.getCause());
                String message = recordInfo.getMessage();
                producer.close();
                properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(sendMessageSize));
                properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(sendMessageSize));
                producer = new KafkaProducer<>(properties);
                alterTopic(sendMessageSize);
                future = sendMessage(producer, record, sent);
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

    private Future<RecordMetadata> sendMessage(KafkaProducer<String, byte[]> producer, ProducerRecord<String, byte[]> record, AtomicBoolean sent) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = producer.send(record, (m, e) -> {
            if (e != null) {
                System.out.println("Send failed for record " + e.getMessage());
                sent.set(false);
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
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "01");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
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
        alterTopic(messageSize);

    }

    @Test
    public void describeTopic() throws InterruptedException, ExecutionException {
        final AdminClient adminClient = AdminClient.create(properties);
        List<ConfigResource> configResources = new ArrayList<>();

        configResources.add(new ConfigResource(ConfigResource.Type.TOPIC, senderTopic));
        configResources.add(new ConfigResource(ConfigResource.Type.TOPIC, senderTopic + "-none"));

        final AtomicInteger done = new AtomicInteger(configResources.size());
        final CompletableFuture<Void> allOfFuture = new CompletableFuture<>();
        final ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<String>();

        DescribeConfigsResult resources = adminClient.describeConfigs(configResources);
        resources.values().forEach((configResource, configKafkaFuture) -> {
            configKafkaFuture.whenComplete((config, throwable) -> {
                if (throwable != null) {
                    if(throwable instanceof UnknownTopicOrPartitionException){
                        System.out.println("Instance of ");
                    }else{
                        System.out.println("resource exception: " + configResource.name() + "-" + throwable.getClass().getCanonicalName());

                    }
                } else {
                    list.add(configResource.name());
                    config.entries().forEach(configEntry -> {
                                System.out.println(configEntry.name()+"|"+configEntry.value());
                            }
                    );
                }
                if (done.decrementAndGet() == 0) {
                    allOfFuture.complete(null);
                }
            });
        });
        allOfFuture.get();
        assertEquals(1,list.size());
    }


    private void alterTopic(int size) throws InterruptedException, ExecutionException {
        String messageSize = String.valueOf(size);
        final AdminClient adminClient = AdminClient.create(properties);

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, senderTopic);
        ConfigEntry maxMessageBytes = new ConfigEntry(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, messageSize);
        ConfigEntry entry = new ConfigEntry(TopicConfig.SEGMENT_BYTES_CONFIG, messageSize);

        List<AlterConfigOp> ops = new ArrayList<>(2);
        AlterConfigOp op = new AlterConfigOp(entry, AlterConfigOp.OpType.SET);
        ops.add(op);
        AlterConfigOp op_max_message = new AlterConfigOp(maxMessageBytes, AlterConfigOp.OpType.SET);
        ops.add(op_max_message);


        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>(2);
        configs.put(configResource, ops);

        KafkaFuture<Void> alterFuture = adminClient.incrementalAlterConfigs(configs).all();
        //final NewTopic newTopic = new NewTopic(senderTopic, Optional.empty(), Optional.empty());
        alterFuture.get();

        assertTrue(alterFuture.isDone());
    }
}
