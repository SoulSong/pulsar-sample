package com.shf.pulsar.schema.keyvalue.inline;

import com.shf.pulsar.schema.keyvalue.BaseProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 11:22
 */
@Slf4j
public class InlineProducer extends BaseProducer implements InlineTopic {

    public static void main(String[] args) throws PulsarClientException {
        produce(KEY_VALUE_SCHEMA,TOPIC_NAME);
    }

}
