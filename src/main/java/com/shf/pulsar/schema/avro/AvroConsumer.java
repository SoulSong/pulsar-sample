package com.shf.pulsar.schema.avro;

import com.shf.pulsar.PulsarClientFactory;
import com.shf.pulsar.schema.IConcumer;
import com.shf.pulsar.schema.User;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.AvroSchema;

import static com.shf.pulsar.schema.IConcumer.consumer;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/7 23:15
 */
public class AvroConsumer implements AvroTopic, IConcumer {

    public static void main(String[] args) throws PulsarClientException {
        Consumer<User> consumer = PulsarClientFactory.createPulsarClient().newConsumer(AvroSchema.of(User.class))
                .topic(TOPIC_NAME)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        consumer(consumer);
    }

}
