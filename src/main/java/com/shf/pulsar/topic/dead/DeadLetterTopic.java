package com.shf.pulsar.topic.dead;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/17 14:31
 */
@Slf4j
public class DeadLetterTopic {

    public static void main(String[] args) throws PulsarClientException {
        String topicName = "persistent://public/default/dead-letter-topic";
        PulsarClient pulsarClient = PulsarClientFactory.createPulsarClient();

        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .topic(topicName)
                // 启用batch，则会导致negativeAcknowledge失效，自动触发ackTimeout，并且也会导致negativeAckRedeliveryDelay无效。
                .enableBatching(false)
                .create();
        log.info("msgId : {}", producer.newMessage().value("msg1").send().toString());
        log.info("msgId : {}", producer.newMessage().value("msg2").send().toString());

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        String subscriptionname = "my-subscription";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .consumerName("origin-Consumer")
                .subscriptionName(subscriptionname)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(10, TimeUnit.SECONDS)
                .negativeAckRedeliveryDelay(1,TimeUnit.MINUTES)
                .enableBatchIndexAcknowledgment(true)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(3)
                        .build())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        // default DLQ name : <topicname>-<subscriptionname>-DLQ
        Consumer<String> dlqConsumer = pulsarClient.newConsumer(Schema.STRING)
                .consumerName("DLQ-Consumer")
                .topic(topicName + "-" + subscriptionname + "-DLQ")
                .subscriptionName(subscriptionname)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        executorService.submit(() -> {
            while (true) {
                Messages<String> messages = consumer.batchReceive();
                messages.forEach(message -> {
                    log.info("consumer : {};msgId : {}; value: {}; PublishTime : {}", consumer.getConsumerName(), message.getMessageId().toString(),
                            message.getValue(), LocalDateTime.ofInstant(Instant.ofEpochMilli(message.getPublishTime()), ZoneId.systemDefault()));
                    try {
                        if ("msg1".equals(message.getValue())) {
                            int i = 1 / 0;
                        }
                        consumer.acknowledge(message);
                    } catch (Exception e) {
                        log.error("invoke error , message : {}", e.getMessage());
                        consumer.negativeAcknowledge(message);
                    }
                });
            }
        });

        executorService.submit(() -> {
            while (true) {
                Messages<String> messages = dlqConsumer.batchReceive();
                messages.forEach(message -> {
                    log.info("consumer : {};msgId : {}; value: {}; PublishTime : {}", dlqConsumer.getConsumerName(), message.getMessageId().toString(),
                            message.getValue(), LocalDateTime.ofInstant(Instant.ofEpochMilli(message.getPublishTime()), ZoneId.systemDefault()));
                    try {
                        dlqConsumer.acknowledge(message);
                    } catch (Exception e) {
                        dlqConsumer.negativeAcknowledge(message);
                    }
                });
            }
        });
    }
}
