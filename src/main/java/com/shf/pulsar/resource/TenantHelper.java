package com.shf.pulsar.resource;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

import java.util.List;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/4 14:11
 */
public class TenantHelper {

    /**
     * 获取所有tenant信息
     *
     * @param pulsarAdmin pulsarAdmin
     * @return list
     * @throws PulsarAdminException e
     */
    public static List<String> listTenants(PulsarAdmin pulsarAdmin) throws PulsarAdminException {
        return pulsarAdmin.tenants().getTenants();
    }

}
