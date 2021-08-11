package com.shf.pulsar.schema.generic;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.util.stream.IntStream;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 12:32
 */
@Slf4j
public class GenericProducer implements GenericTopic {
    public static void main(String[] args) throws PulsarClientException {

        // 构建schemaInfo
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
        recordSchemaBuilder.field("age").type(SchemaType.INT32);
        recordSchemaBuilder.field("name").type(SchemaType.STRING);

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        log.info("SchemaDefinition -> {}", schemaInfo.getSchemaDefinition());

        GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);
        Producer<GenericRecord> producer = PulsarClientFactory.createPulsarClient()
                .newProducer(genericSchema)
                .topic(TOPIC_NAME)
                .create();

        IntStream.rangeClosed(1, 10).forEach(i -> {
            try {
                GenericRecordBuilder genericRecordBuilder = genericSchema.newRecordBuilder();
                MessageId msgId = producer.newMessage()
                        .value(genericRecordBuilder.set("age", i)
                                .set("name", "xiao generic" + i)
                                .build())
                        .send();
                log.info("msgId: {}", msgId.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
