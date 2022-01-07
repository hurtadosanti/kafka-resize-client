import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import static org.awaitility.Awaitility.await;

import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientExamplesTest {

    @Test
    public void testPublishMessage() {
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .buildBlocking();

        client.connect();
        client.publishWith().topic("test/topic").qos(MqttQos.AT_LEAST_ONCE).payload("1".getBytes()).send();
        client.disconnect();
    }

    @Test
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
