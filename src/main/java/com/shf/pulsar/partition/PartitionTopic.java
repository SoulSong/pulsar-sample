package com.shf.pulsar.partition;

import com.shf.pulsar.PulsarClientFactory;
import com.shf.pulsar.resource.TopicHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/4 17:58
 */
@Slf4j
public class PartitionTopic {

    public static void main(String[] args) throws PulsarClientException, PulsarAdminException {
        PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();

        String topicName = "persistent://public/default/my-ptopic";
        TopicHelper.createPartitionedTopic(pulsarAdmin, topicName, 3);

        log.info("{}", TopicHelper.getPartitionedTopicMetadata(pulsarAdmin, topicName).partitions);

        TopicHelper.updatePartitionedTopic(pulsarAdmin, topicName, 5);

        log.info("{}", TopicHelper.getPartitionedTopicMetadata(pulsarAdmin, topicName).partitions);

        Producer<String> producer = PulsarClientFactory.createPulsarClient().newProducer(Schema.STRING)
                .topic(topicName)
                // default is HashingScheme.JavaStringHash
//                .hashingScheme(HashingScheme.Murmur3_32Hash)
//                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                // 可不设置
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new AlwaysThreeRouter())
                .create();

        producer.newMessage().key("1").value("1-1").send();
        producer.newMessage().key("2").value("2-1").send();
        producer.newMessage().key("3").value("3-1").send();
        producer.newMessage().key("2").value("2-2").send();

        // 打印分区统计信息
        PartitionedTopicStats partitionedTopicStats = TopicHelper.getPartitionedStats(pulsarAdmin, topicName, true);
        log.info("{}", partitionedTopicStats.getPartitions());

        TopicHelper.deletePartitionTopic(pulsarAdmin, topicName,true);
    }

    public static class AlwaysThreeRouter implements MessageRouter {
        @Override
        public int choosePartition(Message msg, TopicMetadata metadata) {
            String key = msg.getKey();
            Object value = msg.getValue();
            int numPartitions = metadata.numPartitions();
            return 3;
        }
    }
}
