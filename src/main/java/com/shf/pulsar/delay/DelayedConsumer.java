package com.shf.pulsar.delay;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import static com.shf.pulsar.PulsarClientFactory.createPulsarClient;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/6 18:27
 */
@Slf4j
public class DelayedConsumer extends DelayedTopic {
    public static void main(String[] args) throws PulsarClientException {
        PulsarClient pulsarClient = createPulsarClient();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(TOPIC_NAME)
                // 仅支持Shared、Key_shard模式，其他模式仍然是立即接收
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("group-1")
                .subscribe();

        while (true) {
            Messages<String> messages = consumer.batchReceive();
            for (Message<String> message : messages) {
                log.info(message.getValue());
            }
            consumer.acknowledge(messages);
        }
    }
}
