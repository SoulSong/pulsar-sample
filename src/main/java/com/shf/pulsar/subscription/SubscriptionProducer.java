package com.shf.pulsar.subscription;

import com.shf.pulsar.PulsarClientFactory;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/3 14:16
 */
public class SubscriptionProducer implements BaseSubscription {

    public static void main(String[] args) throws PulsarClientException {
        Producer<String> producer = PulsarClientFactory.createPulsarClient().newProducer(Schema.STRING)
                .topic(TOPIC_NAME)
                .enableBatching(false)
                .create();
        // 3 messages with "key-1", 3 messages with "key-2", 2 messages with "key-3" and 2 messages with "key-4"
        producer.newMessage().key("key-1").value("message-1-1").send();
        producer.newMessage().key("key-1").value("message-1-2").send();
        producer.newMessage().key("key-1").value("message-1-3").send();
        producer.newMessage().key("key-2").value("message-2-1").send();
        producer.newMessage().key("key-2").value("message-2-2").send();
        producer.newMessage().key("key-2").value("message-2-3").send();
        producer.newMessage().key("key-3").value("message-3-1").send();
        producer.newMessage().key("key-3").value("message-3-2").send();
        producer.newMessage().key("key-4").value("message-4-1").send();
        producer.newMessage().key("key-4").value("message-4-2").send();
    }

}
