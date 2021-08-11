package com.shf.pulsar.compact;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.util.stream.IntStream;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/6 19:43
 */
@Slf4j
public class CompactProducer extends CompactTopic {

    public static void main(String[] args) throws PulsarClientException {
        Producer<String> producer = PulsarClientFactory.createPulsarClient().newProducer(Schema.STRING)
                .topic(TOPIC_NAME)
                .create();

        IntStream.rangeClosed(1, 10).forEach(i -> {
            try {
                MessageId msgId = producer.newMessage().key(i % 2 == 0 ? "key-0" : "key-1").value("message-" + i).send();
                log.info("msgId: {}", msgId.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
