package com.shf.pulsar.reader;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.util.stream.IntStream;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/6 15:07
 */
@Slf4j
public class ReaderProducer extends ReaderTopic {

    public static void main(String[] args) throws IOException {
        Producer<String> producer = PulsarClientFactory.createPulsarClient().newProducer(Schema.STRING)
                .topic(TOPIC_NAME)
                .create();
        IntStream.rangeClosed(1, 5).forEach(i -> {
            try {
                MessageId msgId = producer.newMessage().key("key-" + i).value("message-" + i).send();
                log.info("msgId: {}", msgId.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
