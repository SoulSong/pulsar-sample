package com.shf.pulsar.schema.keyvalue.separated;

import com.shf.pulsar.schema.keyvalue.BaseProducer;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 11:32
 */
public class SeparatedProducer extends BaseProducer implements SeparatedTopic {

    public static void main(String[] args) throws PulsarClientException {
        produce(KEY_VALUE_SCHEMA, TOPIC_NAME);
    }

}
