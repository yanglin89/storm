package com.run.stream.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 本地模式的 drpc
 */
public class LocalDRPCTopology {

    /**
     * DRPCSpout 在 drpc 中已经实现，不需要我们在实现
     */

    public static class MyBolt extends BaseRichBolt{

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {

            /**
             * drpc 中传输的tuple 中有两个参数，第一个为 唯一id，第二个为 实际的参数
             */
            Object requestId = input.getValue(0); //请求的id
            String name = input.getString(1); //请求的参数

            /**
             * TODO 处理业务逻辑
             */
            String result = "add user : " + name + " success.";
            this.collector.emit(new Values(requestId,result));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id","result"));
        }
    }


    public static void main(String[] args) {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("userAdd");
        builder.addBolt(new MyBolt());

        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("local_drpc",new Config(),builder.createLocalTopology(drpc));

        String result = drpc.execute("userAdd","zhangsan");
        System.out.println("==================>   from client " +result);

        cluster.shutdown();
        drpc.shutdown();
    }

}
