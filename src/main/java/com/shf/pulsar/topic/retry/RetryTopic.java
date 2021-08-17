package com.shf.pulsar.topic.retry;

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
import java.util.concurrent.TimeUnit;

/**
 * description :
 * 1、没有被ack确认的消息自动进入重试队列；
 * 2、根据reconsumeLater设定的延迟时间重新消费；
 * 3、maxRedeliverCount设定重试队列的重复消费次数；
 * 4、如果重试队列达到最大次数仍然未被ack确认，则消息进入死信队列；
 *
 * @author songhaifeng
 * @date 2021/8/17 14:31
 */
@Slf4j
public class RetryTopic {

    public static void main(String[] args) throws PulsarClientException {
        String topicName = "persistent://public/default/retry-original-topic";
        String retryTopicName = "persistent://public/default/retry-topic";
        PulsarClient pulsarClient = PulsarClientFactory.createPulsarClient();

        Producer<String> producer = PulsarClientFactory.createPulsarClient()
                .newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(true)
                .create();

        log.info("msgId : {}", producer.newMessage().value("msg1").send().toString());
        log.info("msgId : {}", producer.newMessage().value("msg2").send().toString());

        String subscriptionname = "my-subscription";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .consumerName("origin-Consumer")
                .subscriptionName(subscriptionname)
                .subscriptionType(SubscriptionType.Shared)
                .enableRetry(true)
                .ackTimeout(20, TimeUnit.SECONDS)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(2)
                        .retryLetterTopic(retryTopicName)
                        .build())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();


        while (true) {
            Messages<String> messages = consumer.batchReceive();
            messages.forEach(message -> {
                log.info("consumer : {};msgId : {}; value: {}; PublishTime : {}; topicName： {}", consumer.getConsumerName(), message.getMessageId().toString(),
                        message.getValue(), LocalDateTime.ofInstant(Instant.ofEpochMilli(message.getPublishTime()), ZoneId.systemDefault()), message.getTopicName());
                try {
                    if ("msg1".equals(message.getValue())) {
                        int i = 1 / 0;
                    }
                    consumer.acknowledge(message);
                } catch (Exception e) {
                    log.error("invoke error , message : {}", e.getMessage());
                    try {
                        // enableRetry设置为false，则会触发reconsumeLater method not support!
                        consumer.reconsumeLater(message, 15, TimeUnit.SECONDS);
                    } catch (PulsarClientException e1) {
                        e1.printStackTrace();
                    }
                }
            });
        }

    }
}
