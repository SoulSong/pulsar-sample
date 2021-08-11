package com.shf.pulsar.reader;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/6 15:07
 */
@Slf4j
public class ReaderTest extends ReaderTopic {

    public static void main(String[] args) throws IOException {

        Map<String, Object> config = new HashMap<>();
        config.put("topicName", TOPIC_NAME);
        config.put("readerName", "reader001");
        config.put("receiverQueueSize", 2000);

        Reader<String> reader = PulsarClientFactory.createPulsarClient().newReader(Schema.STRING)
                .topic(TOPIC_NAME)
                .startMessageId(MessageId.earliest)
//                .startMessageId(MessageId.latest)
//                .startMessageId(new BatchMessageIdImpl(48645, 2, -1, 0))
                .loadConf(config)
                .keyHashRange(Range.of(0, 65535))
                .create();

        while (true) {
            Message message = reader.readNext();
            log.info("msgId : {} ; key : {} ; value : {}", message.getMessageId().toString(), message.getKey(), message.getValue());
        }
    }
}
