package com.Storm.Ex6_AddingParallelismToStormTopology.Ex2_FieldGrouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.Storm.Ex6_AddingParallelismToStormTopology.Ex1_ShuffleGrouping.IntegerSpout;
import com.Storm.Ex6_AddingParallelismToStormTopology.Ex1_ShuffleGrouping.WriteToFileBolt;

/*
 * The file and bolt used here is from com.Storm.Ex6_AddingParallelismToStormTopology package
 */
public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("Integer-Spout", new IntegerSpout());
		builder.setBolt("Write-To-File-Bolt", new WriteToFileBolt(), 2).fieldsGrouping("Integer-Spout",
				new Fields("bucket"));

		// Configuration
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("dirToWrite", "/home/sauravbilung/Documents/Study/StormOutputs/FieldsGrouping/");

		LocalCluster cluster = new LocalCluster();
		try {
			cluster.submitTopology("Fields-Grouping-Topology", conf, builder.createTopology());
			Thread.sleep(10000);
		} finally {
			cluster.shutdown();
		}

		System.out.println("Program has ended !!!!");
	}

}
