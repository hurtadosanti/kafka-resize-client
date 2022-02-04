import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import static org.awaitility.Awaitility.await;

import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MqttClientIT {
    final static String sendTopic = "ping";
    final static String receiveTopic = "pong";
    final static String host="localhost";

    @Test
    public void testPublishLargeMessage() throws InterruptedException {

        final byte[] largePayload = RandomStringUtils.randomAscii(1024*1024).getBytes(StandardCharsets.UTF_8);

        final int size = largePayload.length;
        System.out.println("Message Size: "+size);
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(host)
                .buildBlocking();

        client.connect();
        final Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);

        System.out.println("Publishing message");
        Executors.newCachedThreadPool().submit(()->{
            sendLargeMessage(largePayload);
            sendLargeMessage(largePayload);
            sendLargeMessage(largePayload);
            sendLargeMessage(largePayload);

        });


        @NotNull Mqtt5Publish largePackageReceived = publishes.receive();
        assertArrayEquals( largePayload,largePackageReceived.getPayloadAsBytes());
        System.out.println("Message received");

    }
    private void sendLargeMessage(byte[] payload){
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(host)
                .buildBlocking();

        client.connect();
        client.subscribeWith().topicFilter(receiveTopic).send();
        client.publishWith().topic(sendTopic).payload(payload).send();
        client.disconnect();
        System.out.println("Message published");
    }
    @Test
    public void testPublishSmallMessage() throws InterruptedException {
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(host)
                .buildBlocking();

        client.connect();

        final Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
        client.subscribeWith().topicFilter(receiveTopic).send();

        client.publishWith().topic(sendTopic).payload("1".getBytes()).send();
        @NotNull Mqtt5Publish smallPackageReceived = publishes.receive();
        assertArrayEquals( "1".getBytes(),smallPackageReceived.getPayloadAsBytes());
        System.out.println("Received small package");
        client.disconnect();
    }
    @Test
    @Timeout(value = 100,unit = TimeUnit.MILLISECONDS)
    public void testSubscribeMessage()  {

        AtomicBoolean received = new AtomicBoolean(false);
        final var asyncClient = Mqtt5Client.builder()
                .serverHost("localhost")
                .buildAsync();
        asyncClient.connect();
        asyncClient.subscribeWith()
                .topicFilter("test/topic")
                .callback(publish -> System.out.println("message received with QoS: "+publish.getQos()))
                .send().join();


        final var blockingClient = Mqtt5Client.builder()
                .serverHost("localhost")
                .buildBlocking();

        blockingClient.connect();


        blockingClient.toAsync().publishes(MqttGlobalPublishFilter.ALL,mqtt5Publish -> {
            @NotNull Optional<ByteBuffer> payload = mqtt5Publish.getPayload();
            if(payload.isPresent()){
                String payloadMessage = StandardCharsets.UTF_8.decode(payload.get().asReadOnlyBuffer()).toString();
                System.out.println(payloadMessage);
                received.set(true);
            }
        });
        blockingClient.subscribeWith().topicFilter("#").send();
        asyncClient.publishWith().topic("test/topic").qos(MqttQos.AT_LEAST_ONCE).payload("Message send".getBytes()).send();
        await().atMost(100, TimeUnit.SECONDS).untilAtomic(received,equalTo(true));
        System.out.println("ended");
        assertTrue(received.get());
    }
}

