package com.Storm.HelloWorldTopology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
		// Build Topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("My-First-spout", new Spout());
		builder.setBolt("My-First-bolt", new Bolt()).shuffleGrouping("My-First-spout");

		// Configuration
		Config conf = new Config();
		conf.setDebug(true); // set to true to see the logs in the console

		// Submit topology to cluster
		LocalCluster cluster = new LocalCluster();
		try {
			cluster.submitTopology("My-First-Topology", conf, builder.createTopology());
			Thread.sleep(1000);
		} finally {
			cluster.shutdown();
		}
	}
}
