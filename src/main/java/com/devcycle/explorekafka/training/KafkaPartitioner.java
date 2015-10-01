package com.devcycle.explorekafka.training;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by chrishowe-jones on 16/09/15.
 */
public class KafkaPartitioner implements Partitioner {

    public KafkaPartitioner(VerifiableProperties props) {
    }

    public int partition(Object key, int a_numpartitions) {
        int partition = 0;
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt(stringKey.substring(offset + 1)) % a_numpartitions;
        }
        return partition;
    }


}
