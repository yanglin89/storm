package com.run.stream;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
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
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * 使用storm 实现累计求和的操作
 */
public class ClusterSumStormAckerTopology {

    /**
     * spout 需要继承 BaseRichSpout
     * spout 需要产生数据并发射
     */
    public static class DataSourceSpot extends BaseRichSpout {

        private SpoutOutputCollector collector;

        /**
         * 初始化方法，之后再初始化的时候被执行一次
         * @param map  配置参数
         * @param topologyContext 上下文
         * @param spoutOutputCollector 数据发射器
         */
        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;
        }

        int number = 0;
        /**
         * 会生产数据，在生产工上从消息队列获取数据
         * 本个案例直接手写数据
         *
         * 该方法是一个死循环，会一直不停的执行
         */
        @Override
        public void nextTuple() {

            ++number;
            /**
             * emit方法有两个参数：
             *  1） 数据
             *  2） 数据的唯一编号 msgId    如果是数据库，msgId就可以采用表中的主键
             */
            this.collector.emit(new Values(number),number);

            System.out.println("spout : " + number);

            Utils.sleep(2000);
        }

        @Override
        public void ack(Object msgId) {
            System.out.println(" ack invoked ..." + msgId);
        }

        @Override
        public void fail(Object msgId) {
            System.out.println(" fail invoked ..." + msgId);

            // TODO... 此处对失败的数据进行重发或者保存下来
            // this.collector.emit(tuple)
            // this.dao.saveMsg(msgId)

        }

        /**
         * 声明输出字段的名称
         * @param outputFieldsDeclarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

            // 此处的 num 和上面的 values 中的值一一对应
            outputFieldsDeclarer.declare(new Fields("num"));
        }
    }


    /**
     * bolt 接受数据并处理
     */
    public static class SumBolt extends BaseRichBolt {

        private OutputCollector collector;

        /**
         *  初始化方法，被执行一次
         * @param map
         * @param topologyContext
         * @param outputCollector
         */
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            /**
             * 因为需要 把 ack 或者 fail 的信息传回到 spout，需要定义 OutputCollector
             */
            this.collector = outputCollector;
        }

        int sum = 0;

        /**
         * 数据执行方法，也是一个死循环
         * 获取 spout 发送过来的数据
         * @param tuple
         */
        @Override
        public void execute(Tuple tuple) {
            // 从 bolt 中获取值，可以通过下标获取，也可以通过spout 中设置的字段名称获取（建议第二种）
            Integer value = tuple.getIntegerByField("num");
            sum += value;
            System.out.println("bolt : " + sum);

            // 假设大于10的就是失败
            if(value > 0 && value <= 10) {
                this.collector.ack(tuple); // 确认消息处理成功
            } else {
                this.collector.fail(tuple);  // 确认消息处理失败
            }


            /**
             * 实际生产中应该 try catch，在里面执行业务逻辑
             */
//            try {
//                // TODO... 你的业务逻辑
//                this.collector.ack(input);
//            } catch (Exception e) {
//                this.collector.fail(input);
//            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }


    public static void main(String[] args) {

        /**
         * TopologyBuilder 是根据spout 和 bolt 来构建出 topology
         * storm 中的任何一个作业都是通过 topology 来提交执行的
         * topology 需要指定 spout 和 bolt 的执行顺序
         */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("dataSourceSpot",new DataSourceSpot());
        builder.setBolt("sumBolt",new SumBolt()).shuffleGrouping("dataSourceSpot");


        /**
         * 提交作业到集群上运行
         */
        String taakName = ClusterSumStormAckerTopology.class.getSimpleName();
        try {
            StormSubmitter.submitTopology(taakName,new Config(),builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }

    }

}