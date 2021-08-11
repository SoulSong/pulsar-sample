package com.shf.pulsar.schema.keyvalue;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;

import java.io.IOException;
import java.util.stream.IntStream;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 11:53
 */
@Slf4j
public abstract class BaseProducer {

    public static void produce(Schema<KeyValue<Integer, String>> keyValueSchema, String topic) throws PulsarClientException {
        Producer<KeyValue<Integer, String>> producer = PulsarClientFactory.createPulsarClient()
                .newProducer(keyValueSchema)
                .topic(topic)
                .create();

        IntStream.rangeClosed(1, 10).forEach(i -> {
            try {
                MessageId msgId = producer.newMessage()
                        .value(new KeyValue<>(i, "value-" + i))
                        .send();
                log.info("msgId: {}", msgId.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
