package com.Storm.Ex6_AddingParallelismToStormTopology.Ex5_DirectGrouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import com.Storm.Ex6_AddingParallelismToStormTopology.Ex1_ShuffleGrouping.WriteToFileBolt;

/*
 * The bolt used here is from com.Storm.Ex6_AddingParallelismToStormTopology package
 */
public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("Direct-Grouping-Integer-Spout", new DirectGroupingIntegerSpout());
		builder.setBolt("Write-To-File-Bolt", new WriteToFileBolt(), 2).directGrouping("Direct-Grouping-Integer-Spout");

		// Configuration
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("dirToWrite", "/home/sauravbilung/Documents/Study/StormOutputs/DirectGrouping/");

		LocalCluster cluster = new LocalCluster();
		try {
			cluster.submitTopology("Direct-Grouping-Topology", conf, builder.createTopology());
			Thread.sleep(10000);
		} finally {
			cluster.shutdown();
		}

		System.out.println("Program has ended !!!!");
	}

}
