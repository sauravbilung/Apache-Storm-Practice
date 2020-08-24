package com.Storm.Ex9_ManagingFailuresInSpout;

import java.util.Map;
import java.util.Random;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class RandomFailureBolt extends BaseRichBolt {

	// Here we are using different abstract class because we want to send failure or
	// acknowledgement messages ourselves to the spout.
	// BaseRichBolt : OutputCollector is also present in the prepared method.
	// This collector will to help in sending the failure or acknowledgement messages.
	// The collector we use in BaseBasicBolt class is not capable of sending
	// failures and acknowledgments by itself.

	private static final long serialVersionUID = 1L;
	private static final Integer MAX_PERCENT_FAAIL = 80;
	Random random = new Random();
	private OutputCollector collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {

		// We are purposely failing the processing of the messages here in some cases
		// so to understand the fault tolerance in Apache Storm.

		Integer r = random.nextInt(100);
		if (r < MAX_PERCENT_FAAIL) {
   			// if failure case does not happenF 
			
			// here we are adding the "input" also a parameter
            collector.emit(input,new Values(input.getString(0),input.getString(1)));
            collector.ack(input);
		}else {
			
			// if the failure case happens
			collector.fail(input);
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("integer","bucket"));
	}

}
