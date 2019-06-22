package com.run.stream.integration.redis;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 使用 storm 完成词频统计
 */
public class LocalWordCountRedisStormTopology {

    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collect;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collect = collector;
        }

        public static final String[] words = new String[]{"apple","hello","hadoop","spark","storm"};

        /**
         * 读取指定目录下的文件，并将每一行数据发射出去
         */
        @Override
        public void nextTuple() {
            Random random = new Random();
            String word = words[random.nextInt(words.length)];

            this.collect.emit(new Values(word));

            System.out.println("emit word :" + word);
            Utils.sleep(2000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }


    /**
     * 对数据进行分割
     */
    public static class SplitBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String line = input.getStringByField("line");

            this.collector.emit(new Values(line));

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }


    public static class CountBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        Map<String,Integer> map = new HashMap<>();
        @Override
        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if (count == null){
                count = 0;
            }
            count++;

            map.put(word,count);

            // 将结果输出
            this.collector.emit(new Values(word,map.get(word)));

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            /**
             * field 中设置的分别为 redis中 key 和 value 对应的标识field
             */
            declarer.declare(new Fields("word","count"));
        }
    }

    /**
     * redis store 操作类
     */
    public static class WordCountStoreMapper implements RedisStoreMapper{

        private RedisDataTypeDescription description;
        private final String hashKey = "wc";

        public WordCountStoreMapper(){
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.HASH,hashKey
            );
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple iTuple) {
            return iTuple.getStringByField("word");
        }

        @Override
        public String getValueFromTuple(ITuple iTuple) {
            return iTuple.getIntegerByField("count")+"";
        }
    }


    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        // 将上一个bolt 中输出的结果写到redis
        /**
         * 此处的 JedisPoolConfig 一定要导入 storm 中的包
         */
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("master").setPort(6379).build();
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig,storeMapper);

        /**
         * 设置 bolt
         */
        builder.setBolt("RedisStoreBolt",storeBolt).shuffleGrouping("CountBolt");

        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountRedisStormTopology",new Config(),builder.createTopology());
    }

}
