package com.shf.pulsar.schema.avro;

import com.shf.pulsar.PulsarClientFactory;
import com.shf.pulsar.schema.User;
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
 * @date 2021/8/7 23:13
 */
@Slf4j
public class AvroProducer implements  AvroTopic{

    public static void main(String[] args) throws PulsarClientException {
        Producer<User> producer = PulsarClientFactory.createPulsarClient().newProducer(Schema.AVRO(User.class))
                .topic(TOPIC_NAME)
                .create();

        IntStream.rangeClosed(1, 10).forEach(i -> {
            try {
                User user = User.builder().id(i).name("xiao avro" + i).age(18 + i).build();
                MessageId msgId = producer.newMessage().key(String.valueOf(user.getId())).value(user).send();
                log.info("msgId: {}", msgId.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
