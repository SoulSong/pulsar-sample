package com.shf.pulsar.resource;

import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;

import java.util.List;
import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/4 14:16
 */
public class TopicHelper {

    /**
     * 查看指定namespace下所有topic列表
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @return list
     * @throws PulsarAdminException e
     */
    public static List<String> listTopics(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        return pulsarAdmin.topics().getList(namespace);
    }

    /**
     * 创建非分区主题
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   建议全名称，包含指定的租户、namespace、持久化信息，如'persistent://my-tenant/my-namespace/my-topic'
     * @throws PulsarAdminException e
     */
    public static void createNonPartitionedTopic(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        pulsarAdmin.topics().createNonPartitionedTopic(topicName);
    }

    /**
     * 删除topic
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static void deleteTopic(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        deleteTopic(pulsarAdmin, topicName, false, false);
    }

    /**
     * 删除topic
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @param force       false下如果当前有活跃的订阅者或者生产者连接当前topic，则无法删除并提示异常'Topic has active producers/subscriptions'；
     *                    反之，force为true则关闭所有的连接并删除topic
     * @throws PulsarAdminException e
     */
    public static void deleteTopic(PulsarAdmin pulsarAdmin, String topicName, boolean force) throws PulsarAdminException {
        deleteTopic(pulsarAdmin, topicName, force, false);
    }

    /**
     * 删除topic
     *
     * @param pulsarAdmin  pulsarAdmin
     * @param topicName    topicName
     * @param force        false下如果当前有活跃的订阅者或者生产者连接当前topic，则无法删除并提示异常'Topic has active producers/subscriptions'；
     *                     反之，force为true则关闭所有的连接并删除topic
     * @param deleteSchema 删除topicSchema信息
     * @throws PulsarAdminException e
     */
    public static void deleteTopic(PulsarAdmin pulsarAdmin, String topicName, boolean force, boolean deleteSchema) throws PulsarAdminException {
        pulsarAdmin.topics().delete(topicName, force, deleteSchema);
    }

    /**
     * 创建分区主题
     *
     * @param pulsarAdmin   pulsarAdmin
     * @param topicName     建议全名称，包含指定的租户、namespace、持久化信息，如'persistent://my-tenant/my-namespace/my-topic'
     * @param numPartitions 分区数
     * @throws PulsarAdminException e
     */
    public static void createPartitionedTopic(PulsarAdmin pulsarAdmin, String topicName, int numPartitions) throws PulsarAdminException {
        pulsarAdmin.topics().createPartitionedTopic(topicName, numPartitions);
    }

    /**
     * 当禁用了topic自动创建功能，且已经有一个没有分区的主题，可以通过createMissedPartitions方法为主题创建分区
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static void createMissedPartitions(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        pulsarAdmin.topics().createMissedPartitions(topicName);
    }

    /**
     * 如果主题是非全局的，可以更新现有已分区主题的分区数量。并且只能添加分区，减少分区的数量就会删除对应主题，在 Pulsar 中是不支持的。
     * 生产者和消费者可以自动找到新创建的分区
     *
     * @param pulsarAdmin   pulsarAdmin
     * @param topicName     topicName
     * @param numPartitions numPartitions
     * @throws PulsarAdminException
     */
    public static void updatePartitionedTopic(PulsarAdmin pulsarAdmin, String topicName, int numPartitions) throws PulsarAdminException {
        pulsarAdmin.topics().updatePartitionedTopic(topicName, numPartitions);
    }

    /**
     * 获取指定topic的分区元数据
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static PartitionedTopicMetadata getPartitionedTopicMetadata(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.topics().getPartitionedTopicMetadata(topicName);
    }

    /**
     * 删除分区topic
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static void deletePartitionTopic(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        pulsarAdmin.topics().deletePartitionedTopic(topicName, false, false);
    }

    /**
     * 删除分区topic
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @param force       false下如果当前有活跃的订阅者或者生产者连接当前topic，则无法删除并提示异常'Topic has active producers/subscriptions'；
     *                    反之，force为true则关闭所有的连接并删除topic
     * @throws PulsarAdminException e
     */
    public static void deletePartitionTopic(PulsarAdmin pulsarAdmin, String topicName, boolean force) throws PulsarAdminException {
        pulsarAdmin.topics().deletePartitionedTopic(topicName, force, false);
    }

    /**
     * 删除分区topic
     *
     * @param pulsarAdmin  pulsarAdmin
     * @param topicName    topicName
     * @param force        false下如果当前有活跃的订阅者或者生产者连接当前topic，则无法删除并提示异常'Topic has active producers/subscriptions'；
     *                     反之，force为true则关闭所有的连接并删除topic
     * @param deleteSchema 删除topicSchema信息
     * @throws PulsarAdminException e
     */
    public static void deletePartitionTopic(PulsarAdmin pulsarAdmin, String topicName, boolean force, boolean deleteSchema) throws PulsarAdminException {
        pulsarAdmin.topics().deletePartitionedTopic(topicName, force, deleteSchema);
    }

    /**
     * 查看某个分区主题的当前统计数据
     *
     * @param pulsarAdmin  pulsarAdmin
     * @param topicName    topicName
     * @param perPartition perPartition
     * @return PartitionedTopicStats
     * @throws PulsarAdminException e
     */
    public static PartitionedTopicStats getPartitionedStats(PulsarAdmin pulsarAdmin, String topicName, boolean perPartition) throws PulsarAdminException {
        return pulsarAdmin.topics().getPartitionedStats(topicName, perPartition);
    }

    /**
     * 设置指定topic是否启用去重功能
     *
     * @param pulsarAdmin         pulsarAdmin
     * @param topicName           topicName
     * @param enableDeduplication enableDeduplication
     * @throws PulsarAdminException e
     */
    public static void setDeduplicationStatus(PulsarAdmin pulsarAdmin, String topicName, boolean enableDeduplication) throws PulsarAdminException {
        pulsarAdmin.topics().setDeduplicationStatus(topicName, enableDeduplication);
    }

    /**
     * 移除指定topic的去重功能
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static void removeDeduplicationStatus(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        pulsarAdmin.topics().removeDeduplicationStatus(topicName);
    }

    /**
     * 设置指定topic去重快照间隔
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @param interval    interval
     * @throws PulsarAdminException e
     */
    public static void setDeduplicationSnapshotInterval(PulsarAdmin pulsarAdmin, String topicName, Integer interval) throws PulsarAdminException {
        pulsarAdmin.topics().setDeduplicationSnapshotInterval(topicName, interval);
    }

    /**
     * 获取指定topic去重快照间隔
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static Integer getDeduplicationSnapshotInterval(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.topics().getDeduplicationSnapshotInterval(topicName);
    }

    /**
     * 删除指定topic去重快照间隔
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static void removeDeduplicationSnapshotInterval(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        pulsarAdmin.topics().removeDeduplicationSnapshotInterval(topicName);
    }

    /**
     * 根据提供的ledger ID 和 entry ID 获取消息
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @param ledgerId    ledgerId
     * @param entryId     entryId
     * @return Message
     * @throws PulsarAdminException e
     */
    public static Message<byte[]> getMessageById(PulsarAdmin pulsarAdmin, String topicName, long ledgerId, long entryId) throws PulsarAdminException {
        return pulsarAdmin.topics().getMessageById(topicName, ledgerId, entryId);
    }

    /**
     * 跳过某一主题的特定订阅的指定数目信息
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @param subName     subName
     * @param numMessages numMessages
     * @throws PulsarAdminException e
     */
    public static void skipMessages(PulsarAdmin pulsarAdmin, String topicName, String subName, long numMessages) throws PulsarAdminException {
        pulsarAdmin.topics().skipMessages(topicName, subName, numMessages);
    }

    /**
     * 跳过某一主题的特定订阅的所有backlog消息
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @param subName     subName
     * @throws PulsarAdminException e
     */
    public static void skipAllMessages(PulsarAdmin pulsarAdmin, String topicName, String subName) throws PulsarAdminException {
        pulsarAdmin.topics().skipAllMessages(topicName, subName);
    }

    /**
     * 重置指定订阅的有效至指定时间戳
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @param subName     subName
     * @param timestamp   timestamp
     * @throws PulsarAdminException e
     */
    public static void resetCursor(PulsarAdmin pulsarAdmin, String topicName, String subName, long timestamp) throws PulsarAdminException {
        pulsarAdmin.topics().resetCursor(topicName, subName, timestamp);
    }

    /**
     * 重置指定订阅的游标至指定的msgId
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @param subName     subName
     * @param messageId   messageId
     * @param isExcluded  isExcluded
     * @throws PulsarAdminException e
     */
    public static void resetCursor(PulsarAdmin pulsarAdmin, String topicName, String subName, MessageId messageId, boolean isExcluded) throws PulsarAdminException {
        pulsarAdmin.topics().resetCursor(topicName, subName, messageId, isExcluded);
    }

    /**
     * 获取指定topic最后一条消息的msgId
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @return MessageId
     * @throws PulsarAdminException e
     */
    public static MessageId getLastMessageId(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.topics().getLastMessageId(topicName);
    }

    /**
     * 获取指定topic的所有订阅
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @return MessageId
     * @throws PulsarAdminException e
     */
    public static List<String> getSubscriptions(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.topics().getSubscriptions(topicName);
    }

    /**
     * 取消指定topic的指定订阅
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static void deleteSubscription(PulsarAdmin pulsarAdmin, String topicName, String subscriptionName) throws PulsarAdminException {
        pulsarAdmin.topics().deleteSubscription(topicName, subscriptionName);
    }

    /**
     * 查看为指定topic提供服务的broker url
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @return the broker URL that serves the topic
     * @throws PulsarAdminException e
     */
    public static String lookupTopic(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.lookups().lookupTopic(topicName);
    }

    /**
     * 查看为指定topic 分区提供服务的broker url
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @return the broker URL that serves the topic
     * @throws PulsarAdminException e
     */
    public static Map<String, String> lookupPartitionedTopic(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.lookups().lookupPartitionedTopic(topicName);
    }

    /**
     * 查看指定topic所属的namespace bundle范围
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @return the namespace bundle range that contains the topic
     * @throws PulsarAdminException e
     */
    public static String getBundleRange(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.lookups().getBundleRange(topicName);
    }

    /**
     * 触发执行主题压缩
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static void triggerCompaction(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        pulsarAdmin.topics().triggerCompaction(topicName);
    }

    /**
     * 查看指定主题压缩执行状态
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static LongRunningProcessStatus compactionStatus(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.topics().compactionStatus(topicName);
    }

    /**
     * 设置主题的自动压缩触发阈值，0表示禁用
     *
     * @param pulsarAdmin         pulsarAdmin
     * @param topicName           topicName
     * @param compactionThreshold maximum number of backlog bytes before compaction is triggered
     * @throws PulsarAdminException e
     */
    public static void setCompactionThreshold(PulsarAdmin pulsarAdmin, String topicName, long compactionThreshold) throws PulsarAdminException {
        pulsarAdmin.topics().setCompactionThreshold(topicName, compactionThreshold);
    }

    /**
     * 获取主题的自动压缩触发阈值，0表示禁用
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @return The maximum number of bytes can have before compaction is triggered
     * @throws PulsarAdminException e
     */
    public static Long getCompactionThreshold(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.topics().getCompactionThreshold(topicName);
    }

    /**
     * 移除指定主题的自动压缩触发阈值
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static void removeCompactionThreshold(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        pulsarAdmin.topics().removeCompactionThreshold(topicName);
    }
}
