package com.shf.pulsar.schema.version;

import com.shf.pulsar.PulsarClientFactory;
import com.shf.pulsar.resource.SchemaHelper;
import com.shf.pulsar.resource.TopicHelper;
import com.shf.pulsar.schema.User;
import com.shf.pulsar.schema.UserV2;
import com.shf.pulsar.schema.UserV3;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * 校验schema version相关逻辑
 */
@Slf4j
public class SchemaUpdateTest {

    /**
     * 开启schema自动更新，并设置不允许schema更改。在进行多次schema更新后，将提示IncompatibleSchemaException异常
     *
     * @throws PulsarClientException
     * @throws PulsarAdminException
     */
    @Test
    public void enableAutoUpdateWithAlwaysInCompatible() throws PulsarClientException, PulsarAdminException {
        String namespace = "public/default";
        String topicName = "persistent://" + namespace + "/enable-auto-update-topic";
        PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();

        TopicHelper.createNonPartitionedTopic(pulsarAdmin, topicName);
        SchemaHelper.setIsAllowAutoUpdateSchema(pulsarAdmin, namespace, true);
        SchemaHelper.setSchemaCompatibilityStrategy(pulsarAdmin, namespace, SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);

        Producer<User> producer = PulsarClientFactory.createPulsarClient()
                .newProducer(Schema.JSON(User.class))
                .topic(topicName)
                .create();
        producer.newMessage().value(User.builder().age(1).name("aa").id(1).build()).send();

        try {
            Producer<UserV2> producer2 = PulsarClientFactory.createPulsarClient()
                    .newProducer(Schema.JSON(UserV2.class))
                    .topic(topicName)
                    .create();
            producer2.newMessage().value(UserV2.builder().age(1).name("aa").id(1).sex("male").build()).send();
        } catch (Exception e) {
            assert e.getMessage().equals("org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException: org.apache.avro.SchemaValidationException: Unable to read schema: \n" +
                    "{\n" +
                    "  \"type\" : \"record\",\n" +
                    "  \"name\" : \"UserV2\",\n" +
                    "  \"namespace\" : \"com.shf.pulsar.schema\",\n" +
                    "  \"fields\" : [ {\n" +
                    "    \"name\" : \"age\",\n" +
                    "    \"type\" : \"int\"\n" +
                    "  }, {\n" +
                    "    \"name\" : \"id\",\n" +
                    "    \"type\" : \"int\"\n" +
                    "  }, {\n" +
                    "    \"name\" : \"name\",\n" +
                    "    \"type\" : [ \"null\", \"string\" ],\n" +
                    "    \"default\" : null\n" +
                    "  }, {\n" +
                    "    \"name\" : \"sex\",\n" +
                    "    \"type\" : [ \"null\", \"string\" ],\n" +
                    "    \"default\" : null\n" +
                    "  } ]\n" +
                    "}\n" +
                    "using schema:\n" +
                    "{\n" +
                    "  \"type\" : \"record\",\n" +
                    "  \"name\" : \"UserV2\",\n" +
                    "  \"namespace\" : \"com.shf.pulsar.schema\",\n" +
                    "  \"fields\" : [ {\n" +
                    "    \"name\" : \"age\",\n" +
                    "    \"type\" : \"int\"\n" +
                    "  }, {\n" +
                    "    \"name\" : \"id\",\n" +
                    "    \"type\" : \"int\"\n" +
                    "  }, {\n" +
                    "    \"name\" : \"name\",\n" +
                    "    \"type\" : [ \"null\", \"string\" ],\n" +
                    "    \"default\" : null\n" +
                    "  }, {\n" +
                    "    \"name\" : \"sex\",\n" +
                    "    \"type\" : [ \"null\", \"string\" ],\n" +
                    "    \"default\" : null\n" +
                    "  } ]\n" +
                    "} caused by org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException: org.apache.avro.SchemaValidationException: Unable to read schema: \n" +
                    "{\n" +
                    "  \"type\" : \"record\",\n" +
                    "  \"name\" : \"UserV2\",\n" +
                    "  \"namespace\" : \"com.shf.pulsar.schema\",\n" +
                    "  \"fields\" : [ {\n" +
                    "    \"name\" : \"age\",\n" +
                    "    \"type\" : \"int\"\n" +
                    "  }, {\n" +
                    "    \"name\" : \"id\",\n" +
                    "    \"type\" : \"int\"\n" +
                    "  }, {\n" +
                    "    \"name\" : \"name\",\n" +
                    "    \"type\" : [ \"null\", \"string\" ],\n" +
                    "    \"default\" : null\n" +
                    "  }, {\n" +
                    "    \"name\" : \"sex\",\n" +
                    "    \"type\" : [ \"null\", \"string\" ],\n" +
                    "    \"default\" : null\n" +
                    "  } ]\n" +
                    "}\n" +
                    "using schema:\n" +
                    "{\n" +
                    "  \"type\" : \"record\",\n" +
                    "  \"name\" : \"UserV2\",\n" +
                    "  \"namespace\" : \"com.shf.pulsar.schema\",\n" +
                    "  \"fields\" : [ {\n" +
                    "    \"name\" : \"age\",\n" +
                    "    \"type\" : \"int\"\n" +
                    "  }, {\n" +
                    "    \"name\" : \"id\",\n" +
                    "    \"type\" : \"int\"\n" +
                    "  }, {\n" +
                    "    \"name\" : \"name\",\n" +
                    "    \"type\" : [ \"null\", \"string\" ],\n" +
                    "    \"default\" : null\n" +
                    "  }, {\n" +
                    "    \"name\" : \"sex\",\n" +
                    "    \"type\" : [ \"null\", \"string\" ],\n" +
                    "    \"default\" : null\n" +
                    "  } ]\n" +
                    "}");
        }

        TopicHelper.deleteTopic(pulsarAdmin, topicName, true, true);
    }

    /**
     * 开启schema自动更新，并允许任意版本。则可以接入任意多个schema版本的producer，以及对应版本的消息。
     *
     * @throws PulsarClientException
     * @throws PulsarAdminException
     */
    @Test
    public void enableAutoUpdateWithAlwaysCompatible() throws PulsarClientException, PulsarAdminException {
        String namespace = "public/default";
        String topicName = "persistent://" + namespace + "/enable-auto-update-topic";
        PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();

        TopicHelper.createNonPartitionedTopic(pulsarAdmin, topicName);
        SchemaHelper.setIsAllowAutoUpdateSchema(pulsarAdmin, namespace, true);
        SchemaHelper.setSchemaCompatibilityStrategy(pulsarAdmin, namespace, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        Producer<User> producer = PulsarClientFactory.createPulsarClient()
                .newProducer(Schema.JSON(User.class))
                .topic(topicName)
                .create();
        producer.newMessage().value(User.builder().age(1).name("aa").id(1).build()).send();

        Producer<UserV2> producer2 = PulsarClientFactory.createPulsarClient()
                .newProducer(Schema.JSON(UserV2.class))
                .topic(topicName)
                .create();
        producer2.newMessage().value(UserV2.builder().age(1).name("aa").id(1).sex("male").build()).send();

        Producer<UserV3> producer3 = PulsarClientFactory.createPulsarClient()
                .newProducer(Schema.JSON(UserV3.class))
                .topic(topicName)
                .create();
        producer3.newMessage().value(UserV3.builder().name("aa").id(1).sex("male").build()).send();

        // list schemas
        List<SchemaInfo> allSchemas = SchemaHelper.getAllSchemas(pulsarAdmin, topicName);
        for (int i = 0; i < allSchemas.size(); i++) {
            SchemaInfo schemaInfo = allSchemas.get(i);
            log.info("schemaInfo : {}", schemaInfo.toString());
            try {
                Long version = SchemaHelper.getVersionBySchema(pulsarAdmin, topicName, schemaInfo);
                log.info("version : {}", version);
                assert i == version;
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        TopicHelper.deleteTopic(pulsarAdmin, topicName, true, true);
    }

    /**
     * 禁用schema自动更新，预定义的topic没有schema。此时含有schema的producer与broker建立连接将失败。
     * 提示：Schema not found and schema auto updating is disabled
     *
     * @throws PulsarClientException
     * @throws PulsarAdminException
     */
    @Test
    public void disableAutoUpdate() throws PulsarClientException, PulsarAdminException {
        String namespace = "public/default";
        String topicName = "persistent://" + namespace + "/disable-auto-update-topic";
        PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();

        TopicHelper.createNonPartitionedTopic(pulsarAdmin, topicName);
        SchemaHelper.setIsAllowAutoUpdateSchema(pulsarAdmin, namespace, false);

        try {
            Producer<User> producer = PulsarClientFactory.createPulsarClient()
                    .newProducer(Schema.JSON(User.class))
                    .topic(topicName)
                    .create();
            producer.newMessage().value(User.builder().age(1).name("aa").id(1).build()).send();
        } catch (PulsarClientException.IncompatibleSchemaException e) {
            assert e.getMessage().equals("org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException: Schema not found and schema auto updating is disabled. caused by org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException: Schema not found and schema auto updating is disabled.");
        }

        TopicHelper.deleteTopic(pulsarAdmin, topicName, true, true);
    }

    /**
     * 强制开启schema校验，则对于已经定义schema的topic，没有schema的producer将无法正常创建连接，提示如下：
     * 提示：Producers cannot connect or send message without a schema to topics with a schema
     *
     * @throws PulsarClientException
     * @throws PulsarAdminException
     */
    @Test
    public void enableSchemaValidation() throws PulsarClientException, PulsarAdminException {
        String namespace = "public/default";
        String topicName = "persistent://" + namespace + "/enable-schema-validation-topic";
        PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();

        TopicHelper.createNonPartitionedTopic(pulsarAdmin, topicName);

        // 创建一个schemaInfo
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
        recordSchemaBuilder.field("age").type(SchemaType.INT32);
        recordSchemaBuilder.field("name").type(SchemaType.STRING);
        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        SchemaHelper.createSchema(pulsarAdmin, topicName, schemaInfo);

        SchemaHelper.setSchemaValidationEnforced(pulsarAdmin, namespace, true);

        try {
            Producer<byte[]> producer = PulsarClientFactory.createPulsarClient()
                    .newProducer()
                    .topic(topicName)
                    .create();
            producer.newMessage().value("aa".getBytes()).send();
        } catch (Exception e) {
            assert e.getMessage().equals("org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException: Producers cannot connect or send message without a schema to topics with a schema caused by org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException: Producers cannot connect or send message without a schema to topics with a schema");
        }

        TopicHelper.deleteTopic(pulsarAdmin, topicName, true, true);
    }

    /**
     * 关闭schema校验，则对于已经定义schema的topic，即时没有schema的producer也能够正常发送消息至当前topic，造成脏数据
     *
     * @throws PulsarClientException
     * @throws PulsarAdminException
     */
    @Test
    public void disableSchemaValidation() throws PulsarClientException, PulsarAdminException {
        String namespace = "public/default";
        String topicName = "persistent://" + namespace + "/disable-schema-validation-topic";
        PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();

        TopicHelper.createNonPartitionedTopic(pulsarAdmin, topicName);

        // 创建一个schemaInfo
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
        recordSchemaBuilder.field("age").type(SchemaType.INT32);
        recordSchemaBuilder.field("name").type(SchemaType.STRING);
        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        SchemaHelper.createSchema(pulsarAdmin, topicName, schemaInfo);

        SchemaHelper.setSchemaValidationEnforced(pulsarAdmin, namespace, false);

        Producer<byte[]> producer = PulsarClientFactory.createPulsarClient()
                .newProducer()
                .topic(topicName)
                .create();
        log.info("msgID: {}", producer.newMessage().value("aa".getBytes()).send().toString());

        TopicHelper.deleteTopic(pulsarAdmin, topicName, true, true);
    }

    /**
     * 设置兼容性为ALWAYS_COMPATIBLE，发布生产多个schema版本的消息至同一topic，而后通过不同version的consumer均能够正常消费消息：
     * 1、使用指定pojo作为schema依据则需要能够容忍字段调整带来的数据丢失、默认值等差异；
     * 2、通过auto_consumer能够保证读取不同pojo完整的数据对象，可通过objectMapper结合version动态反序列化为指定pojo类型；
     *
     * @throws PulsarClientException
     * @throws PulsarAdminException
     */
    @Test
    public void multiSchemaVersionConsumer() throws PulsarClientException, PulsarAdminException {
        String namespace = "public/default";
        String topicName = "persistent://" + namespace + "/multi-schema-version-topic";
        PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();

        TopicHelper.createNonPartitionedTopic(pulsarAdmin, topicName);
        SchemaHelper.setIsAllowAutoUpdateSchema(pulsarAdmin, namespace, true);
        SchemaHelper.setSchemaCompatibilityStrategy(pulsarAdmin, namespace, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        Producer<User> producer = PulsarClientFactory.createPulsarClient()
                .newProducer(Schema.JSON(User.class))
                .topic(topicName)
                .create();
        producer.newMessage().value(User.builder().age(1).name("aa").id(1).build()).send();

        Producer<UserV2> producer2 = PulsarClientFactory.createPulsarClient()
                .newProducer(Schema.JSON(UserV2.class))
                .topic(topicName)
                .create();
        producer2.newMessage().value(UserV2.builder().age(1).name("aa").id(1).sex("male").build()).send();

        Producer<UserV3> producer3 = PulsarClientFactory.createPulsarClient()
                .newProducer(Schema.JSON(UserV3.class))
                .topic(topicName)
                .create();
        producer3.newMessage().value(UserV3.builder().name("aa").id(1).sex("male").build()).send();

        log.info("----------------------consumer with V1--------------------");
        Consumer<User> consumer = PulsarClientFactory.createPulsarClient()
                .newConsumer(Schema.JSON(User.class))
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("my-subscription-1")
                .subscribe();

        for (int i = 0; i < 3; i++) {
            Messages<User> messages = consumer.batchReceive();
            messages.forEach(message -> {
                log.info("msgId : {} ; value: {} ; schema-version : {} ", message.getMessageId().toString(), message.getValue().toString(),
                        SchemaUtils.getStringSchemaVersion(message.getSchemaVersion()));
                try {
                    consumer.acknowledge(message);
                } catch (PulsarClientException e) {
                    log.error("{}", e.getMessage());
                    consumer.negativeAcknowledge(message);
                }
            });
        }
        consumer.close();

        log.info("----------------------consumer with V2--------------------");
        Consumer<UserV2> consumer2 = PulsarClientFactory.createPulsarClient()
                .newConsumer(Schema.JSON(UserV2.class))
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("my-subscription-2")
                .subscribe();

        for (int i = 0; i < 3; i++) {
            Messages<UserV2> messages = consumer2.batchReceive();
            messages.forEach(message -> {
                log.info("msgId : {} ; value: {} ; schema-version : {} ", message.getMessageId().toString(), message.getValue().toString(),
                        SchemaUtils.getStringSchemaVersion(message.getSchemaVersion()));
                try {
                    consumer2.acknowledge(message);
                } catch (PulsarClientException e) {
                    log.error("{}", e.getMessage());
                    consumer2.negativeAcknowledge(message);
                }
            });
        }
        consumer2.close();

        log.info("----------------------consumer with V3--------------------");
        Consumer<UserV3> consumer3 = PulsarClientFactory.createPulsarClient()
                .newConsumer(Schema.JSON(UserV3.class))
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("my-subscription-3")
                .subscribe();

        for (int i = 0; i < 3; i++) {
            Messages<UserV3> messages = consumer3.batchReceive();
            messages.forEach(message -> {
                log.info("msgId : {} ; value: {} ; schema-version : {} ", message.getMessageId().toString(), message.getValue().toString(),
                        SchemaUtils.getStringSchemaVersion(message.getSchemaVersion()));
                try {
                    consumer3.acknowledge(message);
                } catch (PulsarClientException e) {
                    log.error("{}", e.getMessage());
                    consumer3.negativeAcknowledge(message);
                }
            });
        }
        consumer3.close();

        log.info("----------------------consumer with auto--------------------");
        Consumer<GenericRecord> consumer4 = PulsarClientFactory.createPulsarClient()
                .newConsumer(Schema.AUTO_CONSUME())
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("my-subscription-4")
                .subscribe();

        ObjectMapper objectMapper = new ObjectMapper();
        for (int i = 0; i < 3; i++) {
            Messages<GenericRecord> messages = consumer4.batchReceive();
            messages.forEach(message -> {
                String version = SchemaUtils.getStringSchemaVersion(message.getSchemaVersion());
                switch (version) {
                    case "0":
                        try {
                            User user = objectMapper.readValue(message.getValue().getNativeObject().toString(), User.class);
                            Assert.assertEquals("User(id=1, name=aa, age=1)", user.toString());
                            log.info("msgId : {} ; value: {} ; schema-version : {} ", message.getMessageId().toString(), user.toString(), version);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        break;
                    case "1":
                        try {
                            UserV2 userV2 = objectMapper.readValue(message.getValue().getNativeObject().toString(), UserV2.class);
                            Assert.assertEquals("UserV2(id=1, name=aa, age=1, sex=male)", userV2.toString());
                            log.info("msgId : {} ; value: {} ; schema-version : {} ", message.getMessageId().toString(), userV2.toString(), version);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        break;
                    case "2":
                        try {
                            UserV3 userV3 = objectMapper.readValue(message.getValue().getNativeObject().toString(), UserV3.class);
                            Assert.assertEquals("UserV3(id=1, name=aa, sex=male)", userV3.toString());
                            log.info("msgId : {} ; value: {} ; schema-version : {} ", message.getMessageId().toString(), userV3.toString(), version);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        break;
                    default:
                        log.info("No schema mapping for msgId : {} ; value: {} ; ", message.getMessageId().toString(), message.getValue().getNativeObject().toString());
                }
                try {
                    consumer4.acknowledge(message);
                } catch (PulsarClientException e) {
                    log.error("{}", e.getMessage());
                    consumer4.negativeAcknowledge(message);
                }
            });
        }
        consumer4.close();

        TopicHelper.deleteTopic(pulsarAdmin, topicName, true, true);
    }

}