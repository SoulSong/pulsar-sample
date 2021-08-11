package com.shf.pulsar.resource;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;

import java.util.List;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 23:22
 */
public class SchemaHelper {

    /**
     * 设置schema兼容性校验策略
     * bin/pulsar-admin namespaces set-schema-compatibility-strategy --compatibility <compatibility-level> tenant/namespace
     *
     * @param pulsarAdmin pulsarAdmin
     * @param namespace   namespace
     * @param strategy    strategy
     * @throws PulsarAdminException e
     */
    public static void setSchemaCompatibilityStrategy(PulsarAdmin pulsarAdmin, String namespace,
                                                      SchemaCompatibilityStrategy strategy) throws PulsarAdminException {
        pulsarAdmin.namespaces().setSchemaCompatibilityStrategy(namespace, strategy);
    }

    /**
     * 设置是否启用schema自动更新能力
     * bin/pulsar-admin namespaces set-is-allow-auto-update-schema --disable tenant/namespace
     * bin/pulsar-admin namespaces set-is-allow-auto-update-schema --enable tenant/namespace
     *
     * @param pulsarAdmin             pulsarAdmin
     * @param namespace               namespace
     * @param isAllowAutoUpdateSchema isAllowAutoUpdateSchema
     * @throws PulsarAdminException e
     */
    public static void setIsAllowAutoUpdateSchema(PulsarAdmin pulsarAdmin, String namespace,
                                                  boolean isAllowAutoUpdateSchema) throws PulsarAdminException {
        pulsarAdmin.namespaces().setIsAllowAutoUpdateSchema(namespace, isAllowAutoUpdateSchema);
    }

    /**
     * 启用/禁用schema验证
     * bin/pulsar-admin namespaces set-schema-validation-enforce --enable tenant/namespace
     * bin/pulsar-admin namespaces set-schema-validation-enforce --disable tenant/namespace
     *
     * @param pulsarAdmin              pulsarAdmin
     * @param namespace                namespace
     * @param schemaValidationEnforced schemaValidationEnforced
     * @throws PulsarAdminException e
     */
    public static void setSchemaValidationEnforced(PulsarAdmin pulsarAdmin, String namespace,
                                                   boolean schemaValidationEnforced) throws PulsarAdminException {
        pulsarAdmin.namespaces().setSchemaValidationEnforced(namespace, schemaValidationEnforced);
    }

    /**
     * <pre>
     *         RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
     *         recordSchemaBuilder.field("age").type(SchemaType.INT32);
     *         recordSchemaBuilder.field("name").type(SchemaType.STRING);
     *         SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
     * </pre>
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @param schemaInfo  schemaInfo
     * @throws PulsarAdminException e
     */
    public static void createSchema(PulsarAdmin pulsarAdmin, String topicName,
                                    SchemaInfo schemaInfo) throws PulsarAdminException {
        pulsarAdmin.schemas().createSchema(topicName, schemaInfo);
    }

    /**
     * <pre>
     *     PostSchemaPayload payload = new PostSchemaPayload();
     *     payload.setType(SchemaType.AVRO.name());
     *     payload.setSchema("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.foo\",\"fields\":[{\"name\":\"file1\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file2\",\"type\":\"string\",\"default\":null},{\"name\":\"file3\",\"type\":[\"null\",\"string\"],\"default\":\"dfdf\"}]}");
     * </pre>
     *
     * @param pulsarAdmin   pulsarAdmin
     * @param topicName     topicName
     * @param schemaPayload schemaPayload
     * @throws PulsarAdminException
     */
    public static void createSchema(PulsarAdmin pulsarAdmin, String topicName,
                                    PostSchemaPayload schemaPayload) throws PulsarAdminException {
        pulsarAdmin.schemas().createSchema(topicName, schemaPayload);
    }

    /**
     * 获取指定topic所有schemaInfo
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @return Get all version schemas <tt>topic</tt>.
     * @throws PulsarAdminException e
     */
    public static List<SchemaInfo> getAllSchemas(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.schemas().getAllSchemas(topicName);
    }

    /**
     * 获取最后一个版本的schemaInfo
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @return SchemaInfo
     * @throws PulsarAdminException e
     */
    public static SchemaInfo getSchemaInfo(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.schemas().getSchemaInfo(topicName);
    }

    /**
     * 获取最后一个版本的SchemaInfo以及对应version信息
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @return SchemaInfoWithVersion
     * @throws PulsarAdminException e
     */
    public static SchemaInfoWithVersion getSchemaInfoWithVersion(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        return pulsarAdmin.schemas().getSchemaInfoWithVersion(topicName);
    }

    /**
     * 获取指定topic指定version的schemaInfo
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @param version     version
     * @return SchemaInfo
     * @throws PulsarAdminException e
     */
    public static SchemaInfo getSchemaInfo(PulsarAdmin pulsarAdmin, String topicName, long version) throws PulsarAdminException {
        return pulsarAdmin.schemas().getSchemaInfo(topicName, version);
    }

    /**
     * 获取指定schemaInfo的version信息
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @param schemaInfo  schemaInfo
     * @return version
     * @throws PulsarAdminException e
     */
    public static Long getVersionBySchema(PulsarAdmin pulsarAdmin, String topicName, SchemaInfo schemaInfo) throws PulsarAdminException {
        return pulsarAdmin.schemas().getVersionBySchema(topicName, schemaInfo);
    }

    /**
     * 获取指定schemaPayload的version信息
     *
     * @param pulsarAdmin   pulsarAdmin
     * @param topicName     topicName
     * @param schemaPayload schemaPayload
     * @return version
     * @throws PulsarAdminException e
     */
    public static Long getVersionBySchema(PulsarAdmin pulsarAdmin, String topicName, PostSchemaPayload schemaPayload) throws PulsarAdminException {
        return pulsarAdmin.schemas().getVersionBySchema(topicName, schemaPayload);
    }

    /**
     * 删除指定topic的所有schema信息
     *
     * @param pulsarAdmin pulsarAdmin
     * @param topicName   topicName
     * @throws PulsarAdminException e
     */
    public static void deleteSchema(PulsarAdmin pulsarAdmin, String topicName) throws PulsarAdminException {
        pulsarAdmin.schemas().deleteSchema(topicName);
    }
}
