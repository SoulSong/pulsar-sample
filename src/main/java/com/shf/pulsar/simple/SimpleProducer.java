package com.shf.pulsar.simple;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.shf.pulsar.PulsarClientFactory.createPulsarClient;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/7/15 11:28
 */
@Slf4j
public class SimpleProducer extends BaseSimple {
    public static void main(String[] args) throws PulsarClientException {
        PulsarClient pulsarClient =  createPulsarClient();

        // producer配置项 https://pulsar.apache.org/docs/en/client-libraries-java/#configure-producer
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(TOPIC_NAME)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .sendTimeout(10, TimeUnit.SECONDS)
                .compressionType(CompressionType.LZ4)
                .create();

        IntStream.range(1, 5).forEach(i -> {
            String content = String.format("hi-pulsar-%d", i);

            try {
                MessageId msgId = producer.send(content.getBytes());
                log.info(msgId.toString());
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });

        producer.closeAsync()
                .thenRun(() -> System.out.println("Producer closed"))
                .exceptionally((ex) -> {
                    System.err.println("Failed to close producer: " + ex);
                    return null;
                });
        pulsarClient.close();
    }

}
