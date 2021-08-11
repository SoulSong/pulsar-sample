package com.shf.pulsar.simple;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static com.shf.pulsar.PulsarClientFactory.createPulsarClient;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/7/15 18:27
 */
@Slf4j
public class SimpleConsumer extends BaseSimple {
    public static void main(String[] args) throws PulsarClientException {
        PulsarClient pulsarClient = createPulsarClient();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TOPIC_NAME)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("group-1")
                // By default the subscription will be created at the end of the topic
                // 故如果先启动producer，后启动consumer完成订阅，在非earliest模式下无法读取启动consumer订阅前producer生产的消息
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .ackTimeout(10, TimeUnit.SECONDS)
                .negativeAckRedeliveryDelay(2, TimeUnit.MINUTES)
                .subscribe();
        while (true) {
            Messages<byte[]> messages = consumer.batchReceive();
            for (Iterator<Message<byte[]>> iterator = messages.iterator(); iterator.hasNext(); ) {
                Message<byte[]> message = iterator.next();
                // do something
                log.info(new String(message.getData()));
            }
            consumer.acknowledge(messages);
        }
    }
}
