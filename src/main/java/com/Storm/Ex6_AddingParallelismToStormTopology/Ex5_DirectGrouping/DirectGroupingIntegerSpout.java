package com.Storm.Ex6_AddingParallelismToStormTopology.Ex5_DirectGrouping;

import java.util.List;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class DirectGroupingIntegerSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	private Integer i = 0;
	private List<Integer> boltIds;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {

		this.collector = collector;
		this.boltIds = context.getComponentTasks("Write-To-File-Bolt");
	}

	@Override
	public void nextTuple() {

		while (i <= 100) {

			Integer intBucket = (this.i / 10);

			this.collector.emitDirect(boltIds.get(getBoltId(intBucket)),
					new Values(this.i.toString(), intBucket.toString()));
			this.i = this.i + 1;
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("integer", "bucket"));
	}

	public Integer getBoltId(Integer intBucket) {
		
		return intBucket % boltIds.size();
	}
}
