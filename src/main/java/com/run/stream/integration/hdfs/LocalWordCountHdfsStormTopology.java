package com.run.stream.integration.hdfs;

import com.run.stream.ClusterSumStormAckerTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
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
public class LocalWordCountHdfsStormTopology {

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
            Utils.sleep(100);
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

        /**
         * 创建 hdfs bolt
         */
        // use "|" instead of "," for field delimiter
        // 生成的文件中 以 | 分割，例如 ： storm|35
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");

        // sync the filesystem after every 100 tuples，每100 条记录往 hdfs 中写一次
        SyncPolicy syncPolicy = new CountSyncPolicy(10);

        // rotate files when they reach 5MB，每5 兆会划分为一个新的文件
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        /**
         * storm 往 hdfs 上的 /foo 目录下写文件，需要 /foo 有写权限
         * hadoop fs -chmod 777 /foo  /foo 目录赋权限
         */
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/foo/");


        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://192.168.52.138:9000")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        /**
         * 设置 bolt
         */

        builder.setBolt("HdfsBolt",bolt).shuffleGrouping("CountBolt");

        // 创建本地集群
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("LocalWordCountHdfsStormTopology",new Config(),builder.createTopology());

        /**
         * 提交作业到集群上运行
         */
        String taskName = LocalWordCountHdfsStormTopology.class.getSimpleName();
        try {
            StormSubmitter.submitTopology(taskName,new Config(),builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }

}
