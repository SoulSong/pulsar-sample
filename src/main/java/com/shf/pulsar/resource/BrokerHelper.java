package com.shf.pulsar.resource;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;

import java.util.List;
import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/4 16:01
 */
public class BrokerHelper {

    /**
     * 获取正在运行的 broker 中所有可用的活跃 broker。
     *
     * @param pulsarAdmin pulsarAdmin
     * @param clusterName clusterName
     * @return list
     * @throws PulsarAdminException e
     */
    public static List<String> getActiveBrokers(PulsarAdmin pulsarAdmin, String clusterName)
            throws PulsarAdminException {
        return pulsarAdmin.brokers().getActiveBrokers(clusterName);
    }

    /**
     * 列出给定Broker所拥有的命名空间列表
     *
     * @param pulsarAdmin pulsarAdmin
     * @param cluster     cluster
     * @param brokerUrl   brokerUrl
     * @return map
     * @throws PulsarAdminException e
     */
    public static Map<String, NamespaceOwnershipStatus> getOwnedNamespaces(PulsarAdmin pulsarAdmin, String cluster, String brokerUrl)
            throws PulsarAdminException {
        return pulsarAdmin.brokers().getOwnedNamespaces(cluster, brokerUrl);
    }
}
