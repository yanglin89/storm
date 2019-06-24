package com.run.stream.integration.kafka;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * 接受 kafka 的数据进行处理的 bolt
 */
public class LogProcessBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 要使用 ack 机制确保消息被成功消费
     * @param input
     */
    @Override
    public void execute(Tuple input) {

        try{
            /**
             * kafka 0.8 采用 bytes这种方式获取
             */
//            byte[] binaryByField = input.getBinaryByField("bytes");
//            String value = new String(binaryByField);
//            System.out.println("value ===============>" + value);

            /**
             * kafka 0.10+ 采用 getValues() 来获取数据
             */
            String value = input.getValue(4).toString();
            System.out.println("value ===============>" + value);
            String value1 = input.getValues().toString();
            System.out.println("value ===============>" + value1);

            this.collector.ack(input);
        } catch (Exception e){
            this.collector.fail(input);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
