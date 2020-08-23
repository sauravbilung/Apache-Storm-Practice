package com.Storm.FileProcessingTopology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {
		
		// Topology definition
		TopologyBuilder builder=new TopologyBuilder();
		builder.setSpout("File-Reader-Spout", new FileReaderSpout());
		builder.setBolt("Simple-Bolt", new Bolt()).shuffleGrouping("File-Reader-Spout");
		
		// Configuration
		Config conf=new Config();
		conf.setDebug(true);
		conf.put("fileToRead", "/home/sauravbilung/Documents/Study/testFile");
		
		LocalCluster cluster=new LocalCluster();
		try {
		cluster.submitTopology("File-Reader-Topology", conf, builder.createTopology());
		Thread.sleep(1000);
		}finally {
			cluster.shutdown();
		}
	}

}
