package com.shf.pulsar.schema.json;

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
 * @date 2021/8/7 22:41
 */
@Slf4j
public class JsonProducer implements JsonTopic {

    public static void main(String[] args) throws PulsarClientException {
        Producer<User> producer = PulsarClientFactory.createPulsarClient().newProducer(Schema.JSON(User.class))
                .topic(TOPIC_NAME)
                .create();

        IntStream.rangeClosed(1, 10).forEach(i -> {
            try {
                User user = User.builder().id(i).name("xiao json" + i).age(18 + i).build();
                MessageId msgId = producer.newMessage().key(String.valueOf(user.getId())).value(user).send();
                log.info("msgId: {}", msgId.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
