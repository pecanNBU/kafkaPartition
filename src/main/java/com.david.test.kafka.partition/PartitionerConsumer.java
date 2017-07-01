/**
 * Created by Administrator on 2017-6-30.
 */
package com.david.test.kafka.partition;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PartitionerConsumer {
    public static void main(String[] args) {
        String topic = "test_diy_partition";
        ConsumerConnector consumer =
                Consumer.createJavaConsumerConnector(createConsumerConfig());

        Map<String, Integer> topicMap = new HashMap<String, Integer>();

        topicMap.put(topic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumer.createMessageStreams(topicMap);

        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);

        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> mam = it.next();
            if (mam.partition() == 0) {
                System.out.println("消费者消费数据: 【分区号: [" + mam.partition() + "], 存储的消息: [" + new String(mam.message()) + "] 】");
            }
        }
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("group.id", "test_diy_part_group");
        props.put("zookeeper.connect", "192.168.1.32:2181");
        props.put("zookeeper.session.timeout.ms", "5000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        props.put("rebalance.max.retries", "5");
        props.put("rebalance.backoff.ms", "1200");

        return new ConsumerConfig(props);
    }
}
