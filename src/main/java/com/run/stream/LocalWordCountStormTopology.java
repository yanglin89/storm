package com.run.stream;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 使用 storm 完成词频统计
 */
public class LocalWordCountStormTopology {

    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collect;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collect = collector;
        }

        /**
         * 读取指定目录下的文件，并将每一行数据发射出去
         */
        @Override
        public void nextTuple() {

            // 获取所有文件，限定只获取 txt 和 json 格式的文件
            Collection<File> files =  FileUtils.listFiles(
                    new File("E:\\study_data\\storm"),new String[]{"txt","json"},true);
            for(File file : files){
                try {
                    // 获取文件中的所有内容
                    List<String> lines =  FileUtils.readLines(file);
                    // 获取文件中的每一行内容
                    for (String line : lines){
                        // 将每一行内容发射出去
                        this.collect.emit(new Values(line));
                    }

                    /**
                     * 将一个文件处理完成之后要改名，防止死循环重复读取文件
                     */
                    FileUtils.moveFile(file,new File(file.getAbsolutePath() + System.currentTimeMillis()));

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
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
            String[] words = line.split(" ");

            for (String word:words) {
                this.collector.emit(new Values(word));
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }


    public static class CountBolt extends BaseRichBolt {

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

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

            System.out.println("--------------------------");
            for(Map.Entry<String,Integer> entry : map.entrySet()){
                System.out.println("word : "+ entry.getKey() + " --> " + entry.getValue());
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }


    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountStormTopology",new Config(),builder.createTopology());
    }

}
