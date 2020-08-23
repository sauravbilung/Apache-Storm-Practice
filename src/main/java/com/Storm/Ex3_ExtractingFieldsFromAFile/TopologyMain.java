package com.Storm.Ex3_ExtractingFieldsFromAFile;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {
		
		// Topology definition
		TopologyBuilder builder=new TopologyBuilder();
		builder.setSpout("Read-Fields-Spout", new ReadFieldsSpout());
		builder.setBolt("Filter-Fields-Bolt", new FilterFieldsBolt()).shuffleGrouping("Read-Fields-Spout");
		
		// Configuration
		Config conf=new Config();
		conf.setDebug(true);
		conf.put("fileToRead", "/home/sauravbilung/Documents/Study/testFile2.txt");
		
		LocalCluster cluster=new LocalCluster();
		try {
		cluster.submitTopology("Field-extractor-Topology", conf, builder.createTopology());
		Thread.sleep(1000);
		}finally {
			cluster.shutdown();
		}
	}

}
