package com.shf.pulsar.schema.keyvalue.inline;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 11:32
 */
public interface InlineTopic {
    String TOPIC_NAME = "persistent://public/default/keyvalue-inline-topic";

    Schema<KeyValue<Integer, String>> KEY_VALUE_SCHEMA = Schema.KeyValue(
            Schema.INT32,
            Schema.STRING,
            KeyValueEncodingType.INLINE
    );
}
