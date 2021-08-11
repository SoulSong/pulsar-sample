package com.shf.pulsar.schema.json;

import com.shf.pulsar.PulsarClientFactory;
import com.shf.pulsar.schema.IConcumer;
import com.shf.pulsar.schema.User;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import static com.shf.pulsar.schema.IConcumer.consumer;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/7 22:41
 */
public class JsonConsumer implements JsonTopic, IConcumer {

    public static void main(String[] args) throws PulsarClientException {
        Consumer<User> consumer = PulsarClientFactory.createPulsarClient().newConsumer(JSONSchema.of(User.class))
                .topic(TOPIC_NAME)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        consumer(consumer);
    }
}
