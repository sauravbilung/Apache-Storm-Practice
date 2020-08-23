package com.Storm.Ex1_HelloWorldTopology;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Bolt extends BaseBasicBolt{
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		collector.emit(new Values(input.getInteger(0)*2));		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("field"));		
	}

}
