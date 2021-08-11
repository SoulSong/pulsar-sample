package com.shf.pulsar.subscription;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/3 14:24
 */
@Slf4j
public class KeyShardConsumer implements BaseSubscription {

    public static void main(String[] args) throws PulsarClientException {
        Consumer<String> consumer1 = PulsarClientFactory.createPulsarClient().newConsumer(Schema.STRING)
                .topic(TOPIC_NAME)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Consumer<String> consumer2 = PulsarClientFactory.createPulsarClient().newConsumer(Schema.STRING)
                .topic(TOPIC_NAME)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Consumer<String> consumer3 = PulsarClientFactory.createPulsarClient().newConsumer(Schema.STRING)
                .topic(TOPIC_NAME)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        executorService.submit(() -> {
            AtomicInteger count = new AtomicInteger();
            while (true) {
                Message<String> message = null;
                try {
                    message = consumer1.receive();
                    log.info("consume by consumer1 , key : {}; value: {}", message.getKey(), message.getValue());
                    Thread.sleep(5);
                    consumer1.acknowledge(message);
                    if (count.incrementAndGet() == 1) {
                        consumer1.close();
                        break;
                    }
                } catch (PulsarClientException e) {
                    log.error("{}", e.getMessage());
                    if (message != null) {
                        consumer1.negativeAcknowledge(message);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        executorService.submit(() -> {
            while (true) {
                Message<String> message = consumer2.receive();
                log.info("consume by consumer2 , key : {}; value: {}", message.getKey(), message.getValue());
                try {
                    Thread.sleep(5);
                    consumer2.acknowledge(message);
                } catch (PulsarClientException e) {
                    log.error("{}", e.getMessage());
                    consumer2.negativeAcknowledge(message);
                }
            }
        });

        executorService.submit(() -> {
            while (true) {
                Message<String> message = consumer3.receive();
                log.info("consume by consumer3 , key : {}; value: {}", message.getKey(), message.getValue());
                try {
                    Thread.sleep(5);
                    consumer3.acknowledge(message);
                } catch (PulsarClientException e) {
                    log.error("{}", e.getMessage());
                    consumer3.negativeAcknowledge(message);
                }
            }
        });

    }
}
