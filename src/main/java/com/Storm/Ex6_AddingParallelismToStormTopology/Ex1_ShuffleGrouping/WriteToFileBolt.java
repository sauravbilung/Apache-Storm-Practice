package com.Storm.Ex6_AddingParallelismToStormTopology.Ex1_ShuffleGrouping;

import java.io.PrintWriter;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WriteToFileBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private PrintWriter writer;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {

		String fileName = "output" + "-" + context.getThisTaskId() + "-" + context.getThisComponentId() + ".txt";

		try {
			this.writer = new PrintWriter(stormConf.get("dirToWrite").toString() + fileName, "UTF-8");
		} catch (Exception e) {
		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String str = input.getStringByField("integer") + "-" + input.getStringByField("bucket");
		collector.emit(new Values(str));
		writer.println(str);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("field"));
	}

	@Override
	public void cleanup() {
		writer.close();
	}

}
