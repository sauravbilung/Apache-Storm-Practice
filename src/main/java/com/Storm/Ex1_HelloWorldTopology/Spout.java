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
	private Integer i=0;
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector; 		
	}

	@Override
	public void nextTuple() {
		this.collector.emit(new Values(this.i));
		this.i=this.i+1;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("field"));	
	}

}
