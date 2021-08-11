package com.shf.pulsar.resource;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/4 14:14
 */
public class NamespaceHelper {

    /**
     * 查看指定tenant下的所有namespace
     *
     * @param pulsarAdmin pulsarAdmin
     * @param tenant      tenant
     * @return list
     * @throws PulsarAdminException e
     */
    public static List<String> listNamespaces(PulsarAdmin pulsarAdmin, String tenant) throws PulsarAdminException {
        return pulsarAdmin.namespaces().getNamespaces(tenant);
    }

    /**
     * 删除命名空间
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static void deleteNamespace(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        pulsarAdmin.namespaces().deleteNamespace(namespace);
    }

    /**
     * 获取指定namespace的策略
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @return Policies
     * @throws PulsarAdminException e
     */
    public static Policies getPolicies(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        return pulsarAdmin.namespaces().getPolicies(namespace);
    }

    /**
     * 从broker卸载指定namespace
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static void unloadNamespace(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        pulsarAdmin.namespaces().unload(namespace);
    }

    /**
     * 卸载指定namespace bundle，其将在被使用时自动装载。主要应用于个别broker负载过高时卸载个别bundle，并最终实现负载均衡。
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @param bundle      bundle
     * @throws PulsarAdminException e
     */
    public static void unloadNamespaceBundle(PulsarAdmin pulsarAdmin, String namespace, String bundle) throws PulsarAdminException {
        pulsarAdmin.namespaces().unloadNamespaceBundle(namespace, bundle);
    }

    /**
     * 分割namespace bundle，缓解大bundle导致broker负载不均衡的问题
     *
     * @param pulsarAdmin        pulsarAdmin
     * @param namespace          namespace
     * @param bundle             bundle
     * @param unloadSplitBundles unloadSplitBundles
     * @param splitAlgorithmName splitAlgorithmName
     * @throws PulsarAdminException e
     */
    public static void splitNamespaceBundle(PulsarAdmin pulsarAdmin, String namespace, String bundle, boolean unloadSplitBundles, String splitAlgorithmName) throws PulsarAdminException {
        pulsarAdmin.namespaces().splitNamespaceBundle(namespace, bundle, unloadSplitBundles, splitAlgorithmName);
    }

    /**
     * 清除属于特定名称空间的所有主题的所有消息积压。
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static void clearNamespaceBacklog(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        pulsarAdmin.namespaces().clearNamespaceBacklog(namespace);
    }

    /**
     * 清除属于特定namespace的 特定订阅的 所有主题的 所有消息积压（backlog）。
     *
     * @param pulsarAdmin  pulsarAdmin
     * @param namespace    namespace
     * @param subscription subscription
     * @throws PulsarAdminException e
     */
    public static void clearNamespaceBundleBacklogForSubscription(PulsarAdmin pulsarAdmin, String namespace, String subscription) throws PulsarAdminException {
        pulsarAdmin.namespaces().clearNamespaceBacklogForSubscription(namespace, subscription);
    }

    /**
     * 清除指定namespace bundle下的所有backlog消息
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @param bundle      bundle
     * @throws PulsarAdminException e
     */
    public static void clearNamespaceBacklogForSubscription(PulsarAdmin pulsarAdmin, String namespace, String bundle) throws PulsarAdminException {
        pulsarAdmin.namespaces().clearNamespaceBundleBacklog(namespace, bundle);
    }

    /**
     * 清除指定namespace bundle下指定订阅的backlog消息
     *
     * @param pulsarAdmin  pulsarAdmin
     * @param namespace    namespace
     * @param bundle       bundle
     * @param subscription subscription
     * @throws PulsarAdminException e
     */
    public static void clearNamespaceBundleBacklogForSubscription(PulsarAdmin pulsarAdmin, String namespace, String bundle, String subscription) throws PulsarAdminException {
        pulsarAdmin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle, subscription);
    }

    /**
     * 设置特定namespace的保留策略
     *
     * @param pulsarAdmin            pulsarAdmin
     * @param namespace              the special namespace
     * @param retentionTimeInMinutes how long to retain messages
     * @param retentionSizeInMB      retention backlog limit
     * @throws PulsarAdminException e
     */
    public static void setRetention(PulsarAdmin pulsarAdmin, String namespace, int retentionTimeInMinutes, int retentionSizeInMB) throws PulsarAdminException {
        pulsarAdmin.namespaces().setRetention(namespace, new RetentionPolicies(retentionTimeInMinutes, retentionSizeInMB));
    }

    /**
     * 获取特定namespace的保留策略
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @return RetentionPolicies
     * @throws PulsarAdminException e
     */
    public static RetentionPolicies getRetention(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        return pulsarAdmin.namespaces().getRetention(namespace);
    }

    /**
     * 设置指定namespace的backlog保留策略
     *
     * @param pulsarAdmin     pulsarAdmin
     * @param namespace       namespace
     * @param limitSize       limitSize
     * @param limitTime       limitTime
     * @param retentionPolicy {@link BacklogQuota.RetentionPolicy}
     * @throws PulsarAdminException e
     */
    public static void setBacklogQuota(PulsarAdmin pulsarAdmin, String namespace, long limitSize,
                                       int limitTime, BacklogQuota.RetentionPolicy retentionPolicy) throws PulsarAdminException {
        pulsarAdmin.namespaces().setBacklogQuota(namespace, BacklogQuota.builder()
                .limitSize(limitSize)
                .limitTime(limitTime)
                .retentionPolicy(retentionPolicy)
                .build());
    }

    /**
     * 查看给定命名空间已配置的backlog保留策略
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @return map
     * @throws PulsarAdminException e
     */
    public static Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        return pulsarAdmin.namespaces().getBacklogQuotaMap(namespace);
    }

    /**
     * 移除指定命名空间的backlog保留策略
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static void removeBacklogQuota(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        pulsarAdmin.namespaces().removeBacklogQuota(namespace);
    }

    /**
     * 设置指定namespace的TTL策略
     *
     * @param pulsarAdmin  pulsarAdmin
     * @param namespace    namespace
     * @param ttlInSeconds TTL values for all messages for all topics in this namespace
     * @throws PulsarAdminException e
     */
    public static void setNamespaceMessageTTL(PulsarAdmin pulsarAdmin, String namespace, int ttlInSeconds) throws PulsarAdminException {
        pulsarAdmin.namespaces().setNamespaceMessageTTL(namespace, ttlInSeconds);
    }

    /**
     * 获取指定namespace的TTL策略
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static Integer getNamespaceMessageTTL(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        return pulsarAdmin.namespaces().getNamespaceMessageTTL(namespace);
    }

    /**
     * 移除指定namespace的TTL策略
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static void removeNamespaceMessageTTL(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        pulsarAdmin.namespaces().removeNamespaceMessageTTL(namespace);
    }

    /**
     * 设置指定namespace是否启用去重功能
     *
     * @param pulsarAdmin         pulsarAdmin
     * @param namespace           namespace
     * @param enableDeduplication enableDeduplication
     * @throws PulsarAdminException e
     */
    public static void setDeduplicationStatus(PulsarAdmin pulsarAdmin, String namespace, boolean enableDeduplication) throws PulsarAdminException {
        pulsarAdmin.namespaces().setDeduplicationStatus(namespace, enableDeduplication);
    }

    /**
     * 移除指定namespace的去重功能
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static void removeDeduplicationStatus(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        pulsarAdmin.namespaces().removeDeduplicationStatus(namespace);
    }

    /**
     * 设置指定namespace去重快照间隔
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @param interval    interval
     * @throws PulsarAdminException e
     */
    public static void setDeduplicationSnapshotInterval(PulsarAdmin pulsarAdmin, String namespace, Integer interval) throws PulsarAdminException {
        pulsarAdmin.namespaces().setDeduplicationSnapshotInterval(namespace, interval);
    }

    /**
     * 获取指定namespace去重快照间隔
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static Integer getDeduplicationSnapshotInterval(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        return pulsarAdmin.namespaces().getDeduplicationSnapshotInterval(namespace);
    }

    /**
     * 删除指定namespace去重快照间隔
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static void removeDeduplicationSnapshotInterval(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        pulsarAdmin.namespaces().removeDeduplicationSnapshotInterval(namespace);
    }

    /**
     * 设置指定 namespace 下所有 topic 的消息派发速率
     * default value see {@link org.apache.pulsar.common.policies.data.impl.DispatchRateImpl}
     *
     * @param pulsarAdmin                 pulsarAdmin
     * @param namespace                   namespace
     * @param dispatchThrottlingRateMsg   每 X 秒派发的消息数量
     * @param dispatchThrottlingRateBytes 每 X 秒派发消息的总字节数
     * @param ratePeriodInSeconds         对应上述的X，默认值为1
     * @throws PulsarAdminException e
     */
    public static void setDispatchRate(PulsarAdmin pulsarAdmin, String namespace, int dispatchThrottlingRateMsg,
                                       long dispatchThrottlingRateBytes, int ratePeriodInSeconds) throws PulsarAdminException {
        pulsarAdmin.namespaces().setDispatchRate(namespace,
                DispatchRate.builder()
                        .dispatchThrottlingRateInMsg(dispatchThrottlingRateMsg)
                        .dispatchThrottlingRateInByte(dispatchThrottlingRateBytes)
                        .ratePeriodInSecond(ratePeriodInSeconds)
                        .build());
    }

    /**
     * 获取指定 namespace 下所有 topic 的消息派发速率
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @return DispatchRate
     * @throws PulsarAdminException e
     */
    public static DispatchRate getDispatchRate(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        return pulsarAdmin.namespaces().getDispatchRate(namespace);
    }

    /**
     * 移除指定 namespace 下所有 topic 的消息派发速率
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static void removeDispatchRate(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        pulsarAdmin.namespaces().removeDispatchRate(namespace);
    }

    /**
     * 设置指定 namespace 下所有订阅的消息派发速率
     * default value see {@link org.apache.pulsar.common.policies.data.impl.DispatchRateImpl}
     *
     * @param pulsarAdmin                 pulsarAdmin
     * @param namespace                   namespace
     * @param dispatchThrottlingRateMsg   每 X 秒派发的消息数量
     * @param dispatchThrottlingRateBytes 每 X 秒派发消息的总字节数
     * @param ratePeriodInSeconds         对应上述的X，默认值为1
     * @throws PulsarAdminException e
     */
    public static void setSubscriptionDispatchRate(PulsarAdmin pulsarAdmin, String namespace, int dispatchThrottlingRateMsg,
                                                   long dispatchThrottlingRateBytes, int ratePeriodInSeconds) throws PulsarAdminException {
        pulsarAdmin.namespaces().setSubscriptionDispatchRate(namespace,
                DispatchRate.builder()
                        .dispatchThrottlingRateInMsg(dispatchThrottlingRateMsg)
                        .dispatchThrottlingRateInByte(dispatchThrottlingRateBytes)
                        .ratePeriodInSecond(ratePeriodInSeconds)
                        .build());
    }

    /**
     * 获取指定 namespace 下所有订阅的消息派发速率
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @return DispatchRate
     * @throws PulsarAdminException e
     */
    public static DispatchRate getSubscriptionDispatchRate(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        return pulsarAdmin.namespaces().getSubscriptionDispatchRate(namespace);
    }

    /**
     * 移除指定 namespace 下所有订阅的消息派发速率
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static void removeSubscriptionDispatchRate(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        pulsarAdmin.namespaces().removeSubscriptionDispatchRate(namespace);
    }

    /**
     * 设置指定 namespace 下所有 Replicator 的消息派发速率
     * default value see {@link org.apache.pulsar.common.policies.data.impl.DispatchRateImpl}
     *
     * @param pulsarAdmin                 pulsarAdmin
     * @param namespace                   namespace
     * @param dispatchThrottlingRateMsg   每 X 秒派发的消息数量
     * @param dispatchThrottlingRateBytes 每 X 秒派发消息的总字节数
     * @param ratePeriodInSeconds         对应上述的X，默认值为1
     * @throws PulsarAdminException e
     */
    public static void setReplicatorDispatchRate(PulsarAdmin pulsarAdmin, String namespace, int dispatchThrottlingRateMsg,
                                                 long dispatchThrottlingRateBytes, int ratePeriodInSeconds) throws PulsarAdminException {
        pulsarAdmin.namespaces().setReplicatorDispatchRate(namespace,
                DispatchRate.builder()
                        .dispatchThrottlingRateInMsg(dispatchThrottlingRateMsg)
                        .dispatchThrottlingRateInByte(dispatchThrottlingRateBytes)
                        .ratePeriodInSecond(ratePeriodInSeconds)
                        .build());
    }

    /**
     * 获取指定 namespace 下所有 Replicator 的消息派发速率
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @return DispatchRate
     * @throws PulsarAdminException e
     */
    public static DispatchRate getReplicatorDispatchRate(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        return pulsarAdmin.namespaces().getReplicatorDispatchRate(namespace);
    }

    /**
     * 移除指定 namespace 下所有 Replicator 的消息派发速率
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @throws PulsarAdminException e
     */
    public static void removeReplicatorDispatchRate(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        pulsarAdmin.namespaces().removeReplicatorDispatchRate(namespace);
    }

    /**
     * 为指定命名空间设置Replication集群，因此 Pulsar 可以从一个 colo 复制消息到另一个 colo。
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace name
     * @param clusterIds  Pulsar Cluster Ids, like ["us-west", "us-east", "us-cent"]
     * @throws PulsarAdminException e
     */
    public static void setNamespaceReplicationClusters(PulsarAdmin pulsarAdmin, String namespace, Set<String> clusterIds) throws PulsarAdminException {
        pulsarAdmin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);
    }

    /**
     * 获取给定命名空间Replication集群的列表。
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace name
     * @return Pulsar Cluster Ids, like ["us-west", "us-east", "us-cent"]
     * @throws PulsarAdminException e
     */
    public static List<String> getNamespaceReplicationClusters(PulsarAdmin pulsarAdmin, String namespace) throws PulsarAdminException {
        return pulsarAdmin.namespaces().getNamespaceReplicationClusters(namespace);
    }
}
