package com.shf.pulsar.schema.keyvalue.separated;

import com.shf.pulsar.schema.keyvalue.BaseConsumer;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 11:32
 */
public class SeparatedConsumer extends BaseConsumer implements SeparatedTopic {

    public static void main(String[] args) throws PulsarClientException {
        consume(KEY_VALUE_SCHEMA, TOPIC_NAME);
    }

}
