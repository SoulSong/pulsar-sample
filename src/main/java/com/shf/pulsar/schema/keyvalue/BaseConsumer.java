package com.shf.pulsar.schema.keyvalue;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.schema.KeyValue;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 11:49
 */
@Slf4j
public abstract class BaseConsumer {

    public static void consume(Schema<KeyValue<Integer, String>> keyValueSchema, String topic) throws PulsarClientException {
        Consumer<KeyValue<Integer, String>> consumer = PulsarClientFactory.createPulsarClient()
                .newConsumer(keyValueSchema)
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("my-subscription")
                .subscribe();

        while (true) {
            Message<KeyValue<Integer, String>> message = consumer.receive();
            KeyValue<Integer, String> keyValue = message.getValue();
            log.info("msgId : {} ; msgKey : {}; msgValue: {} ; ", message.getMessageId().toString(), message.getKey(), message.getValue().toString());
            log.info("msgId : {} ; key : {}; value: {} ; ", message.getMessageId().toString(), keyValue.getKey(), keyValue.getValue());
            try {
                consumer.acknowledge(message);
            } catch (PulsarClientException e) {
                log.error("{}", e.getMessage());
                consumer.negativeAcknowledge(message);
            }
        }

    }

}
