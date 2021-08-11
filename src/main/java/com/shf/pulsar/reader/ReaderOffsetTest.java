package com.shf.pulsar.reader;

import com.shf.pulsar.PulsarClientFactory;
import com.shf.pulsar.resource.TopicHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.util.MessageIdUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * description :
 * 通过offset管理messageId
 *
 * @author songhaifeng
 * @date 2021/8/10 16:04
 */
@Slf4j
public class ReaderOffsetTest {

    public static void main(String[] args) throws IOException, PulsarAdminException {
        PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();

        String topicName = "persistent://public/default/message-id-topic";

        Producer<String> producer = PulsarClientFactory.createPulsarClient().newProducer(Schema.STRING)
                .topic(topicName)
                .create();

        List<Long> messageIdOffset = new ArrayList<>(5);
        AtomicLong maxOffset = new AtomicLong();
        IntStream.rangeClosed(1, 5).forEach(i -> {
            try {
                MessageId msgId = producer.newMessage().key("key-" + i).value("message-" + i).send();
                long offset = MessageIdUtils.getOffset(msgId);
                log.info("msgId: {}, offset: {}", msgId.toString(), offset);
                messageIdOffset.add(offset);
                maxOffset.set(offset);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        producer.close();

        Reader<String> reader = PulsarClientFactory.createPulsarClient().newReader(Schema.STRING)
                .topic(topicName)
                .startMessageId(MessageIdUtils.getMessageId(messageIdOffset.get(2)))
                .create();

        while (true) {
            Message message = reader.readNext();
            log.info("msgId : {} ; key : {} ; value : {}", message.getMessageId().toString(), message.getKey(), message.getValue());
            if (maxOffset.get() == MessageIdUtils.getOffset(message.getMessageId())) {
                break;
            }
        }
        reader.close();
        TopicHelper.deleteTopic(pulsarAdmin, topicName);
        pulsarAdmin.close();
    }

}
