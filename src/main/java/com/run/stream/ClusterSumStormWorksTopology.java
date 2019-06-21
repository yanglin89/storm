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
public class ClusterSumStormWorksTopology {

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

            this.collector.emit(new Values(++number));

            System.out.println("spout : " + number);

            Utils.sleep(2000);
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

        /**
         *  初始化方法，被执行一次
         * @param map
         * @param topologyContext
         * @param outputCollector
         */
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

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
        builder.setSpout("dataSourceSpot",new DataSourceSpot(),2); //设置 excutor 的数量为2，即每个worker里面有两个 excutor
        builder.setBolt("sumBolt",new SumBolt(),2)  //设置 excutor 的数量为2，即每个worker里面有两个 excutor
                .setNumTasks(4) //设置task 的运行数量，即相当于2个 excutor中启动了4个task来执行bolt
                .shuffleGrouping("dataSourceSpot");


        /**
         * 提交作业到集群上运行
         */
        String taskName = ClusterSumStormWorksTopology.class.getSimpleName();
        try {
            Config config = new Config();
            config.setNumWorkers(2); // 设置worker 数量 （进程级别，会占用默认的4个端口中的两个）
//            config.setNumAckers(0); //将 ack关闭，默认是打开的（打开才能保证数据是否被重复发送）
            config.setNumAckers(1); //和不设置效果一样，都默认打开，ack的数量和worker挂钩，每个worker对应一个ack。 而且ack还会占用task数量
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
