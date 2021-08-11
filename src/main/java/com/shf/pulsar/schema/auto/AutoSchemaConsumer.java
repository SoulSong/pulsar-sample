package com.shf.pulsar.schema.auto;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 15:59
 */
@Slf4j
public class AutoSchemaConsumer implements AutoSchemaTopic {

    public static void main(String[] args) throws PulsarClientException {
        Consumer<GenericRecord> consumer = PulsarClientFactory.createPulsarClient()
                .newConsumer(Schema.AUTO_CONSUME())
                .topic(TOPIC_NAME)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Message<GenericRecord> message = consumer.receive();
        log.info("msgId : {}; id : {}; age: {}; name : {}; schemaType : {}",
                message.getMessageId().toString(),
                message.getValue().getField("id"),
                message.getValue().getField("age"),
                message.getValue().getField("name"),
                message.getValue().getSchemaType());
    }
}
