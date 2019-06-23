package com.run.stream.integration.hbase;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 使用 storm 完成词频统计
 */
public class LocalWordCountHbaseStormTopology {

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

        Config config = new Config();
        /**
         * 设置 hbase 的基本参数conf
         */
        Map<String,Object> hbaseConf = new HashMap<>();
        hbaseConf.put("hbase.rootdir","hdfs://master:9000/hbase");
        hbaseConf.put("hbase.zookeeper.quorum","master:2181");

        config.put("hbase.conf",hbaseConf);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        // 将上一个bolt 中输出的结果写到 hbase
        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("word")   // hbase 表的 rowkey ，此处我们使用的是上一步 bolt 中declare 的 word 的字段
                .withColumnFields(new Fields("word"))  // hbase 的cf 中的字段名
                .withCounterFields(new Fields("count"))  // hbase 的cf 中的字段名
                .withColumnFamily("cf");  // hbase 的cf

        /**
         * 此处要设置 ConfigKey 为hbase 的配置文件的 key
         */
        HBaseBolt hbase = new HBaseBolt("wc", mapper).withConfigKey("hbase.conf");

        /**
         * 设置 bolt
         */
        builder.setBolt("HbaseBolt",hbase).shuffleGrouping("CountBolt");

        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountHbaseStormTopology",config,builder.createTopology());
    }

}
