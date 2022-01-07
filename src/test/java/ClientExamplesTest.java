import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import org.junit.jupiter.api.Test;

import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

import java.util.UUID;

public class ClientExamplesTest {

    @Test
    public void testPublishMessage(){
        System.out.println("Hello");
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .buildBlocking();

        client.connect();
        client.publishWith().topic("test/topic").qos(MqttQos.AT_LEAST_ONCE).payload("1".getBytes()).send();
        client.disconnect();
    }
}
