package com.shf.pulsar.resource;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;

@Slf4j
public class TopicHelperTest {
    PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();

    public TopicHelperTest() throws PulsarClientException {
    }

    /**
     * 根据提供的ledger ID 和 entry ID 获取消息
     *
     * @throws PulsarAdminException e
     */
    @Test
    public void getMessageById() throws PulsarAdminException {
        Message<byte[]> message = TopicHelper.getMessageById(pulsarAdmin, "reader-topic", 48645, 2);
        log.info("msgId : {} ; key : {} ; value : {}", message.getMessageId().toString(), message.getKey(), new String(message.getValue()));
    }

    /**
     * 获取指定topic最后一条消息的msgId
     *
     * @throws PulsarAdminException e
     */
    @Test
    public void getLastMessageId() throws PulsarAdminException {
        log.info("{}", TopicHelper.getLastMessageId(pulsarAdmin, "reader-topic").toString());
    }

    /**
     * 获取指定topic的所有订阅
     *
     * @throws PulsarAdminException e
     */
    @Test
    public void getSubscriptions() throws PulsarAdminException {
        TopicHelper.getSubscriptions(pulsarAdmin, "reader-topic").forEach(log::info);
    }

    /**
     * 获取指定topic的所有订阅
     *
     * @throws PulsarAdminException e
     */
    @Test
    public void deleteSubscription() throws PulsarAdminException {
        TopicHelper.deleteSubscription(pulsarAdmin, "reader-topic", " my-subscription");
    }

    @Test
    public void lookupTopic() throws PulsarAdminException {
        log.info(TopicHelper.lookupTopic(pulsarAdmin, "reader-topic"));
    }

    @Test
    public void lookupPartitionedTopic() throws PulsarAdminException {
        String topicName = "persistent://public/default/my-ptopic";
        TopicHelper.createPartitionedTopic(pulsarAdmin, topicName, 3);
        log.info("{}", TopicHelper.lookupPartitionedTopic(pulsarAdmin, "persistent://public/default/my-ptopic"));
        TopicHelper.deletePartitionTopic(pulsarAdmin,topicName);
    }

    @Test
    public void getBundleRange() throws PulsarAdminException {
        log.info(TopicHelper.getBundleRange(pulsarAdmin, "reader-topic"));
    }
}