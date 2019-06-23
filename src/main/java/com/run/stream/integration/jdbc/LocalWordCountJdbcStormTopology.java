package com.run.stream.integration.jdbc;

import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
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
public class LocalWordCountJdbcStormTopology {

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
             * field 中设置的 名称要与 数据库中 字段的名称一样
             */
            declarer.declare(new Fields("WORD","WORD_COUNT"));
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

        // 将上一个bolt 中输出的结果写到 jdbc
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://master/storm_jdbc");
        hikariConfigMap.put("dataSource.user","hadoop");
        hikariConfigMap.put("dataSource.password","mdhc5891");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "wc_storm";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);

        /*JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("insert into user values (?,?)")
                .withQueryTimeoutSecs(30);*/

        /**
         * 设置 bolt
         */
        builder.setBolt("RedisStoreBolt",userPersistanceBolt).shuffleGrouping("CountBolt");

        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountJdbcStormTopology",new Config(),builder.createTopology());
    }

}
