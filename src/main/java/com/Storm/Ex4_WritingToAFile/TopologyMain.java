package com.Storm.Ex4_WritingToAFile;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("Read-Fields-Spout", new ReadFieldsSpout());
		builder.setBolt("Filter-Fields-To-File-Bolt", new FilterFieldsToFileBolt())
				.shuffleGrouping("Read-Fields-Spout");

		// Configuration
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("fileToRead", "/home/sauravbilung/Documents/Study/testFile2.txt");
		conf.put("dirToWrite", "/home/sauravbilung/Documents/Study/StormOutputs/");

		LocalCluster cluster = new LocalCluster();
		try {
			cluster.submitTopology("Write-to-File-Topology", conf, builder.createTopology());
			Thread.sleep(10000);
		} finally {
			cluster.shutdown();
		}
		
		System.out.println("Program has ended !!!!");
	}

}
