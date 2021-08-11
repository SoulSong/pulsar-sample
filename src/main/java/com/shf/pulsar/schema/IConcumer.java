package com.shf.pulsar.schema;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/7 23:08
 */
public interface IConcumer {
    Logger LOGGER = LoggerFactory.getLogger(IConcumer.class);

    static <T> void consumer(Consumer<T> consumer) throws PulsarClientException {
        while (true) {
            Messages<T> messages = consumer.batchReceive();
            messages.forEach(message -> {
                LOGGER.info("msgId : {} ; key : {}; value: {} ; ", message.getMessageId().toString(), message.getKey(), message.getValue().toString());
                try {
                    consumer.acknowledge(message);
                } catch (PulsarClientException e) {
                    LOGGER.error("{}", e.getMessage());
                    consumer.negativeAcknowledge(message);
                }
            });
        }
    }

}
