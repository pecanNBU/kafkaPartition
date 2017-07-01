package com.david.test.kafka.partition;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by Administrator on 2017-6-30.
 */

public class SimplePartitioner implements Partitioner {
    /**
     * 不写这个方法，会报错
     * Exception in thread "main" java.lang.NoSuchMethodException: ckm.kafka.producer.SimplePartitioner.<init>(kafka.utils.VerifiableProperties)
     * at java.lang.Class.getConstructor0(Class.java:2892)
     * at java.lang.Class.getConstructor(Class.java:1723)
     * at kafka.utils.Utils$.createObject(Utils.scala:436)
     * at kafka.producer.Producer.<init>(Producer.scala:61)
     * at kafka.javaapi.producer.Producer.<init>(Producer.scala:26)
     * at ckm.kafka.producer.KafkaProducerWithPartition.<init>(KafkaProducerWithPartition.java:58)
     * at ckm.kafka.producer.KafkaProducerWithPartition.main(KafkaProducerWithPartition.java:70)
     *
     * @param verifiableProperties
     */
    public SimplePartitioner(VerifiableProperties verifiableProperties) {

    }

    public int partition(Object key, int numPartitions) {
        int partition = 0;

        String k = (String) key;

        partition = Math.abs(k.hashCode()) % numPartitions;

        return partition;
    }
}
