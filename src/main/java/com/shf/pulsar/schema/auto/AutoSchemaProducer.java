package com.shf.pulsar.schema.auto;

import com.shf.pulsar.PulsarClientFactory;
import com.shf.pulsar.schema.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 15:59
 */
@Slf4j
public class AutoSchemaProducer implements AutoSchemaTopic {

    public static void main(String[] args) throws PulsarClientException, org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        Producer<byte[]> producer = PulsarClientFactory.createPulsarClient()
                .newProducer(Schema.AUTO_PRODUCE_BYTES())
                .topic(TOPIC_NAME)
                .create();

        producer.newMessage()
                .value(objectMapper.writeValueAsBytes(User.builder().id(1).name("auto_producer").build()))
                .send();

        // 无效消息验证
//        MessageId messageId = producer.newMessage()
//                .value("abc".getBytes())
//                .send();
//        log.info("msgId : {}", messageId.toString());
    }
}
