package com.shf.pulsar.compact;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * description :
 * 验证主题压缩功能，验证之前先执行手动压缩：
 * bin/pulsar-admin --admin-url http://localhost:18080  topics compact persistent://public/default/compact-topic
 *
 * @author songhaifeng
 * @date 2021/8/6 19:43
 */
@Slf4j
public class CompactConsumer extends CompactTopic {

    public static void main(String[] args) throws PulsarClientException {
        Consumer<String> compactedConsumer = PulsarClientFactory.createPulsarClient().newConsumer(Schema.STRING)
                .topic(TOPIC_NAME)
                .subscriptionName("my-subscription-1")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .readCompacted(true)
                .consumerName("compactedConsumer")
                .subscribe();

        Consumer<String> normalConsumer = PulsarClientFactory.createPulsarClient().newConsumer(Schema.STRING)
                .topic(TOPIC_NAME)
                .subscriptionName("my-subscription-2")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("normalConsumer")
                .subscribe();

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        executorService.submit(() -> {
            try {
                consumer(compactedConsumer);
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });

        executorService.submit(() -> {
            try {
                consumer(normalConsumer);
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
    }

    public static void consumer(Consumer<String> consumer) throws PulsarClientException {
        while (true) {
            Messages<String> messages = consumer.batchReceive();
            messages.forEach(message -> {
                log.info("consumerName : {}; msgId : {} ; key : {}; value: {} ; ", consumer.getConsumerName(), message.getMessageId().toString(),
                        message.getKey(), message.getValue());
                try {
                    consumer.acknowledge(message);
                } catch (PulsarClientException e) {
                    log.error("{}", e.getMessage());
                    consumer.negativeAcknowledge(message);
                }
            });
        }
    }
}
