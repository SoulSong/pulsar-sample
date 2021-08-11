package com.shf.pulsar.subscription;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.TimeUnit;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/3 14:24
 */
@Slf4j
public class ExclusiveConsumer implements  BaseSubscription{

    public static void main(String[] args) throws PulsarClientException {
        Consumer<String> consumer = PulsarClientFactory.createPulsarClient().newConsumer(Schema.STRING)
                .topic(TOPIC_NAME)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

//        Consumer<String> consumer2 = PulsarClientFactory.createPulsarClient().newConsumer(Schema.STRING)
//                .topic(TOPIC_NAME)
//                .subscriptionName("my-subscription")
//                .subscriptionType(SubscriptionType.Exclusive)
//                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
//                .subscribe();

        while(true){
            Messages<String> messages = consumer.batchReceive();
            messages.forEach(message->{
                log.info("key : {}; value: {}",message.getKey(),message.getValue());
                try {
                    consumer.acknowledge(message);
                } catch (PulsarClientException e) {
                    log.error("{}",e.getMessage());
                    consumer.negativeAcknowledge(message);
                }
            });
        }
    }
}
