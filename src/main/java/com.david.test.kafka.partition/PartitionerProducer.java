package com.david.test.kafka.partition;
/**
 * Created by Administrator on 2017-6-30.
 */

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;


public class PartitionerProducer {

    //kafka bin 目录下运行下列命令
    /*./kafka-topics.sh \
            --create \
            --zookeeper 192.168.1.32:2181 \
            --replication-factor 1 \
            --partitions 4 \
            --topic test_diy_partition
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "192.168.1.32:9092");
        props.put("partitioner.class", "com.david.test.kafka.partition.SimplePartitioner");

        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));

        String topic = "test_diy_partition";
        while (true) {
            for (int i = 0; i <= 10; i++) {
                String k = "key" + i;
                String v = k + "--value" + i;
                producer.send(new KeyedMessage<String, String>(topic, k, v));
            }
        }
        //producer.close();
    }


}
