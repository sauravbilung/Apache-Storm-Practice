package com.Storm.Ex9_ManagingFailuresInSpout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		// Topology Builder
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("Integer-Spout", new IntegerSpout());
		builder.setBolt("Random-Failure-Bolt", new RandomFailureBolt()).shuffleGrouping("Integer-Spout");

		// Configuration
		Config conf = new Config();
		conf.setDebug(true);

		// Topology run
		LocalCluster cluster = new LocalCluster();
		try {
			cluster.submitTopology("Random-Fail-Topology", conf, builder.createTopology());
			Thread.sleep(10000);
		} finally {
			cluster.shutdown();
		}

		System.out.println("Program has ended !!!");
	}

}
