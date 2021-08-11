package com.shf.pulsar.resource;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

import java.util.List;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/4 15:58
 */
public class ClusterHelper {

    /**
     * 可以获取 Pulsar 实例中的所有集群。
     *
     * @param pulsarAdmin pulsarAdmin
     * @return list
     * @throws PulsarAdminException e
     */
    public static List<String> getClusters(PulsarAdmin pulsarAdmin) throws PulsarAdminException {
        return pulsarAdmin.clusters().getClusters();
    }
}
