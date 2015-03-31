package com.tencent.trt.storm.test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.Grouping;
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
import java.util.*;

/**
 * Created by wentao on 1/24/15.
 */
public class StormDirectGrouping extends TestCase implements Serializable {

    public void test() throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout", new BaseRichSpout() {
            private SpoutOutputCollector collector;
            private boolean noMore;
            public TopologyContext context;
            private Map<String, Map<String, Grouping>> targets;
            public HashMap<String, Map<String, List<Integer>>> downstreamTasks;

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declare(true, new Fields("data"));
            }

            @Override
            public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
                this.collector = collector;
                this.context = context;
                targets = context.getThisTargets();
                this.downstreamTasks = new HashMap<String, Map<String, List<Integer>>>();
                for (Map.Entry<String, Map<String, Grouping>> entry : targets.entrySet()) {
                    String streamName = entry.getKey();
                    HashMap<String, List<Integer>> xxx = new HashMap<String, List<Integer>>();
                    downstreamTasks.put(streamName, xxx);
                    for (String downstreamComponentId : entry.getValue().keySet()) {
                        List<Integer> tasks = context.getComponentTasks(downstreamComponentId);
                        Collections.sort(tasks);
                        xxx.put(downstreamComponentId, tasks);
                    }
                }
                System.out.println(downstreamTasks);
            }

            @Override
            public void nextTuple() {
                if (noMore) {
                    return;
                }
                noMore = true;
                List<Object> data = new ArrayList<Object>();
                for (int i = 0; i < 100; i++) {
                    data.add(i);
                }
                Map<String, List<Integer>> comps = downstreamTasks.get("default");
                for (Map.Entry<String, List<Integer>> entry : comps.entrySet()) {
                    List<Integer> taskIds = entry.getValue();
                    for (Object ele : data) {
                        int hashTo = ele.hashCode() % taskIds.size();
                        Integer hashToTaskId = taskIds.get(hashTo);
                        collector.emitDirect(hashToTaskId, Arrays.asList(new Object[]{ele}));
                    }
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
        }, 3).directGrouping("spout");
        LocalCluster localCluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);
        localCluster.submitTopology("local", conf, topologyBuilder.createTopology());
        Thread.sleep(60 * 1000);
    }
}
