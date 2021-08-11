package com.shf.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.HashMap;
import java.util.Map;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/3 14:20
 */
public class PulsarClientFactory {
    static final String SERVICE_URL = "pulsar://192.168.3.166:6650";
    /**
     * 如果有多个broker，String url = "http://localhost:8080,localhost:8081,localhost:8082";
     */
    static final String ADMIN_URL = "http://192.168.3.166:18080";

    public static PulsarClient createPulsarClient() throws PulsarClientException {
        // 更多配置项参考https://pulsar.apache.org/docs/en/client-libraries-java/#client
        ClientBuilder builder = PulsarClient.builder().ioThreads(2);
        Map<String, Object> config = new HashMap<>();
        config.put("numIoThreads", 20);

        return builder
                .serviceUrl(SERVICE_URL)
                .loadConf(config)
                .build();
    }

    public static PulsarAdmin createPulsarAdmin() throws PulsarClientException {
        return PulsarAdmin.builder()
                .serviceHttpUrl(ADMIN_URL)
                .build();
    }

}
