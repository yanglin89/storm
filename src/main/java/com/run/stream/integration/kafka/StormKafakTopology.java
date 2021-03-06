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
                        .setProp("enabled.idempotence",true)  //设置消息幂等，只消费一次，且保证消费
//                        .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.NO_GUARANTEE) //没有开启ack，在指定interval定期commit，异步提交
//                        .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE) //开启ack，在指定interval定期commit，同步提交
//                        .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE) //没有开启ack，在polled out消息的时候同步commit(忽略interval配置)，因此消息只处理一次
                        .build()), 1);

        String BOLT_ID = LogProcessBolt.class.getSimpleName();
        builder.setBolt(BOLT_ID,new LogProcessBolt()).shuffleGrouping("kafka_spout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(StormKafakTopology.class.getSimpleName(),
                new Config(),
                builder.createTopology());
    }

}
