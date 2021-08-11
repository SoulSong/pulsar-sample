package com.shf.pulsar.reader;

import com.shf.pulsar.PulsarClientFactory;
import com.shf.pulsar.resource.TopicHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * description :
 * 验证consumer  的resetCursor 和 skip message行为
 *
 * @author songhaifeng
 * @date 2021/8/6 15:08
 */
@Slf4j
public class SkipConsumer extends ReaderTopic {

    public static void main(String[] args) throws PulsarClientException, PulsarAdminException {

        String subscriptionName = "my-subscription";
        Consumer<String> consumer = PulsarClientFactory.createPulsarClient().newConsumer(Schema.STRING)
                .topic(TOPIC_NAME)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();

        TopicHelper.resetCursor(pulsarAdmin, TOPIC_NAME, subscriptionName,
                new BatchMessageIdImpl(48645, 2, -1, 0), true);

//        TopicHelper.resetCursor(pulsarAdmin, TOPIC_NAME, subscriptionName,
//                LocalDateTime.now().minusHours(3).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

        TopicHelper.skipMessages(pulsarAdmin, TOPIC_NAME, subscriptionName, 4);

        consumer(consumer);
    }

    public static void consumer(Consumer<String> consumer) throws PulsarClientException {
        while (true) {
            Messages<String> messages = consumer.batchReceive();
            messages.forEach(message -> {
                log.info("msgId : {} ; key : {}; value: {} ; PublishTime : {}", message.getMessageId().toString(),
                        message.getKey(), message.getValue(),
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(message.getPublishTime()), ZoneId.systemDefault()));
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
