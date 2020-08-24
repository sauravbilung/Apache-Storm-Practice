package com.Storm.Ex7_WordCountTopology;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class WordCounterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	Integer id;
	String name;
	Map<String, Integer> counters;
	String fileName;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {

		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		this.fileName = stormConf.get("dirToWrite").toString() + "output" + "-" + id + "-" + name + ".txt";
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String str = input.getString(0);
		/**
		 * If the word dosn't exist in the map we will create this, if not We will add 1
		 */
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {

		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");

			for (Map.Entry<String, Integer> entry : counters.entrySet()) {
				writer.println(entry.getKey() + ": " + entry.getValue());
			}
			writer.close();
		 } catch (Exception e) {
		 }
	}

}
