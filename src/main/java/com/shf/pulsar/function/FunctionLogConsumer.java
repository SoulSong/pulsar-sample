package com.shf.pulsar.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.Iterator;

import static com.shf.pulsar.PulsarClientFactory.createPulsarClient;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/21 20:27
 */
@Slf4j
public class FunctionLogConsumer {

    public static void main(String[] args) throws PulsarClientException {
        Consumer<byte[]> consumer = createPulsarClient().newConsumer()
                .topic("persistent://public/default/logging-function-logs")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("group-1")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        while (true) {
            Messages<byte[]> messages = consumer.batchReceive();
            for (Iterator<Message<byte[]>> iterator = messages.iterator(); iterator.hasNext(); ) {
                Message<byte[]> message = iterator.next();
                log.info(new String(message.getData()));
            }
            consumer.acknowledge(messages);
        }
    }
}
