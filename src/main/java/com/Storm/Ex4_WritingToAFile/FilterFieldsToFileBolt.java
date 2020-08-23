package com.Storm.Ex4_WritingToAFile;

import java.io.PrintWriter;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FilterFieldsToFileBolt extends BaseBasicBolt {

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

		String firstName = input.getStringByField("first_name");
		String lastName = input.getString(2);

		writer.println(firstName + "," + lastName);
		collector.emit(new Values(firstName, lastName));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("first_name", "last_name"));
	}

	@Override
	public void cleanup() {
		writer.close();
	}

}
