package com.run.stream.integration.kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;

/**
 * storm 整合 kafka
 */
public class StormKafakTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka_spout", new KafkaSpout<>(
                KafkaSpoutConfig.builder(
                        "master:9093,master:9094,master:9095", "storm_topic")
                        .setGroupId("testgroup")  // 设置 groupid
                        .setProp("enabled.idempotence",true)  //设置消息只消费一次，且保证消费
                        .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE) //设置消息只消费一次，且保证消费
                        .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE) //设置消息只消费一次，且保证消费
                        .build()), 1);

        String BOLT_ID = LogProcessBolt.class.getSimpleName();
        builder.setBolt(BOLT_ID,new LogProcessBolt()).shuffleGrouping("kafka_spout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(StormKafakTopology.class.getSimpleName(),
                new Config(),
                builder.createTopology());
    }

}
