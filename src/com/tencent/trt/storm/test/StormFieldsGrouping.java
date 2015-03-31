package com.tencent.trt.storm.test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import junit.framework.TestCase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by wentao on 1/24/15.
 */
public class StormFieldsGrouping extends TestCase implements Serializable {

    public void test() throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout", new BaseRichSpout() {
            private SpoutOutputCollector collector;
            private boolean noMore;

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(new Fields("partition", "data"));
            }

            @Override
            public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

                this.collector = collector;
            }

            @Override
            public void nextTuple() {
                if (noMore) {
                    return;
                }
                noMore = true;
                for (int i = 0; i < 100; i++) {
                    final int finalI = i;
                    collector.emit(new ArrayList<Object>() {{
                        add(finalI);
                        add(finalI);
                    }});
                }
            }
        });
        topologyBuilder.setBolt("bolt", new BaseRichBolt() {

            private int taskId;
            private int taskIndex;

            @Override
            public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
                taskId = context.getThisTaskId();
                taskIndex = context.getThisTaskIndex();
            }

            @Override
            public void execute(Tuple input) {
                System.out.println(taskId + " " + taskIndex + " " + input);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {

            }
        }, 3).fieldsGrouping("spout", new Fields("partition"));
        LocalCluster localCluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);
        localCluster.submitTopology("local", conf, topologyBuilder.createTopology());
        Thread.sleep(60 * 1000);
    }
}
