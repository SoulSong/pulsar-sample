package com.shf.pulsar.resource;

import com.shf.pulsar.PulsarClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;

import java.util.List;
import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/3 19:04
 */
@Slf4j
public class AdminTest {

    public static void main(String[] args) throws PulsarClientException, PulsarAdminException {
        PulsarAdmin pulsarAdmin = PulsarClientFactory.createPulsarAdmin();
        log.info("all clusters");
        log.info("-----------------");
        List<String> clusters = ClusterHelper.getClusters(pulsarAdmin);
        clusters.forEach(log::info);
        log.info("-----------------\n");

        log.info("all active brokers");
        log.info("-----------------");
        List<String> brokers = BrokerHelper.getActiveBrokers(pulsarAdmin, clusters.get(0));
        brokers.forEach(log::info);
        log.info("-----------------\n");

        log.info("all assigned namespace in active brokers[{}]", brokers.get(0));
        log.info("-----------------");
        Map<String, NamespaceOwnershipStatus> status = BrokerHelper.getOwnedNamespaces(pulsarAdmin, clusters.get(0), brokers.get(0));
        log.info(status.toString());
        log.info("-----------------\n");

        log.info("all tenants:");
        log.info("-----------------");
        List<String> tenants = TenantHelper.listTenants(pulsarAdmin);
        tenants.forEach(log::info);
        log.info("-----------------\n");

        log.info("all namespaces:");
        log.info("-----------------");
        tenants.forEach(tenant -> {
            try {
                log.info("tenant : {}", tenant);
                List<String> namespaces = NamespaceHelper.listNamespaces(pulsarAdmin, tenant);
                namespaces.forEach(namespace -> {
                    try {
                        log.info("{}=>{}", namespace, NamespaceHelper.getPolicies(pulsarAdmin, namespace));
                    } catch (PulsarAdminException e) {
                        e.printStackTrace();
                    }
                });
                log.info("-----------------");
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        });
        log.info("\n");

        log.info("all topics:");
        log.info("-----------------");
        tenants.forEach(tenant -> {
            try {
                List<String> namespaces = NamespaceHelper.listNamespaces(pulsarAdmin, tenant);
                namespaces.forEach(namespace -> {
                    try {
                        List<String> topics = TopicHelper.listTopics(pulsarAdmin, namespace);
                        topics.forEach(log::info);
                        log.info("-----------------");
                    } catch (PulsarAdminException e) {
                        e.printStackTrace();
                    }
                });
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        });
    }

}
