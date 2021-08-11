package com.shf.pulsar.schema.generic;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 12:32
 */
@Slf4j
public class GenericConsumer implements GenericTopic {

    public static void main(String[] args) throws PulsarClientException {
        // 构建schemaInfo
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
        recordSchemaBuilder.field("age").type(SchemaType.INT32);
        recordSchemaBuilder.field("name").type(SchemaType.STRING);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        log.info("SchemaDefinition -> {}", schemaInfo.getSchemaDefinition());

        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

        Consumer<GenericRecord> consumer = PulsarClientFactory.createPulsarClient()
                .newConsumer(genericSchema)
                .topic(TOPIC_NAME)
                .subscriptionName("my-subscription")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        while (true) {
            Messages<GenericRecord> messages = consumer.batchReceive();
            messages.forEach(message -> {
                log.info("msgId : {}; key : {}; age: {}; name : {}; schemaType : {}",
                        message.getMessageId().toString(), message.getKey(), message.getValue().getField("age"),
                        message.getValue().getField("name"), message.getValue().getSchemaType());
                try {
                    consumer.acknowledge(message);
                } catch (PulsarClientException e) {
                    log.error("{}", e.getMessage());
                    consumer.negativeAcknowledge(message);
                }
            });
        }
    }


}
