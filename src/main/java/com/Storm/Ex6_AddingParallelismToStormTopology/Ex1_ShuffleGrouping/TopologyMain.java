package com.Storm.Ex6_AddingParallelismToStormTopology.Ex1_ShuffleGrouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("Integer-Spout", new IntegerSpout());
		// We created 2 bolt instances here and data is distributed to these two bolts.
		// Data distribution is based on grouping strategies.
		// In shuffle grouping strategy a random bolt is selected and data is forwarded. 
		builder.setBolt("Write-To-File-Bolt", new WriteToFileBolt(), 2).shuffleGrouping("Integer-Spout");

		// Configuration
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("dirToWrite", "/home/sauravbilung/Documents/Study/StormOutputs/ShuffleGrouping/");

		LocalCluster cluster = new LocalCluster();
		try {
			cluster.submitTopology("Shuffle-Grouping-Topology", conf, builder.createTopology());
			Thread.sleep(10000);
		} finally {
			cluster.shutdown();
		}

		System.out.println("Program has ended !!!!");
	}

}
