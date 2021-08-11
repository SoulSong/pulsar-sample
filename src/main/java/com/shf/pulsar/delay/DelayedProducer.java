package com.shf.pulsar.delay;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.shf.pulsar.PulsarClientFactory.createPulsarClient;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/6 11:28
 */
@Slf4j
public class DelayedProducer extends DelayedTopic {
    public static void main(String[] args) throws PulsarClientException {
        PulsarClient pulsarClient = createPulsarClient();

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(TOPIC_NAME)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        IntStream.range(1, 5).forEach(i -> {
            try {
//                producer.newMessage()
//                        .deliverAfter(3L, TimeUnit.MINUTES)
//                        .value("Send message-" + i + " at " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
//                        .send();
                producer.newMessage()
                        .deliverAt(LocalDateTime.now().plusMinutes(1).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                        .value("Send message-" + i + " at " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                        .send();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });

        pulsarClient.close();
    }

}
