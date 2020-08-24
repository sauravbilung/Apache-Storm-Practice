package com.Storm.Ex9_ManagingFailuresInSpout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class IntegerSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	// This for keeping track of how many times the failure has occurred.
	private Map<Integer, Integer> integerFailureCount;
	private List<Integer> toSend;
	private static Integer MAX_FAILS =3;

	static Logger logs = Logger.getLogger(IntegerSpout.class);

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {

		this.toSend = new ArrayList<Integer>();

		for (int i = 0; i < 100; i++) {
			toSend.add(i);
		}

		this.integerFailureCount = new HashMap<Integer, Integer>();
		this.collector = collector;
	}

	@Override
	public void nextTuple() {

		if (!toSend.isEmpty()) {
			for (Integer current : toSend) {
				// generating the bucket for data distribution.
				Integer intBucket = (current / 10);
				// current parameter can be the message or a transaction id.
				// If further downstream the tuple fails then using this we would to which
				// original
				// message the tuple belongs to. By default storm generates a random id when it
				// emits
				// a tuple. But since we want to explicitly track the messages that are sent by
				// the spout
				// we have added it.
				this.collector.emit(new Values(current.toString(), intBucket.toString()), current);
			}
			toSend.clear();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("integer", "bucket"));
	}

	@Override
	public void ack(Object msgId) {

		// called when the method is successfully processed by the topology.
		System.out.println(msgId + " Successful");
	}

	@Override
	public void fail(Object msgId) {

		// called when the message is not successfully processed by the topology
		// msgId is the id of the tuple that failed to be processed. But in our
		// case it is the integer number that got failed.

		Integer failures = 1;
		Integer failedId = (Integer) msgId;

		if (integerFailureCount.containsKey(failedId)) {
			failures = integerFailureCount.get(failedId) + 1;
		}
		
		if(failures < MAX_FAILS) {
			integerFailureCount.put(failedId, failures);
			toSend.add(failedId);
			logs.info("Re-sending message ["+failedId+"]");
		} else {
			logs.info("Sending message ["+failedId+"] failed");
		}
		
	}

}
