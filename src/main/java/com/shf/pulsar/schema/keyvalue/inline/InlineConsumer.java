package com.shf.pulsar.schema.keyvalue.inline;

import com.shf.pulsar.schema.keyvalue.BaseConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/8/8 11:32
 */
@Slf4j
public class InlineConsumer extends BaseConsumer implements InlineTopic {

    public static void main(String[] args) throws PulsarClientException {
        consume(KEY_VALUE_SCHEMA, TOPIC_NAME);
    }

}
