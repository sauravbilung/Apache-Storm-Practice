package com.Storm.Ex6_AddingParallelismToStormTopology.Ex4_CustomGrouping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

public class BucketGrouping implements CustomStreamGrouping, Serializable {

	private static final long serialVersionUID = 1L;
	private List<Integer> targetTasks;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {

		// Evenly distributing the tasks to the bolts using the below logic.

		List<Integer> boltIds = new ArrayList<Integer>();
		Integer boltNum = Integer.parseInt(values.get(1).toString()) % targetTasks.size();
		boltIds.add(targetTasks.get(boltNum));
		return boltIds;
	}

}
