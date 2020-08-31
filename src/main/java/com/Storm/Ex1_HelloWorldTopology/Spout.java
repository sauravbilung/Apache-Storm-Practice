package com.Storm.Ex1_HelloWorldTopology;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Spout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private Integer i = 0;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {

		// Here spout gets the topology information via conf and context variables.
		// The values of these two variables are configured at the topology level.
		// These method initializes the spout.
		// SpoutOutput Collector is setup and managed by the storm cluster for spout and
		// for every bolt. Collector collects the event coming to the component and pass
		// this on to the next component.
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		// Here events(data in layman terms) are processed.
		// This method is continuously called by the storm cluster to process the events.
		// It emits data to the next component in the topology.
		this.collector.emit(new Values(this.i));
		this.i = this.i + 1;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		// Declares the output tuple schema which is passed to the next component.
		declarer.declare(new Fields("field"));
	}

}
