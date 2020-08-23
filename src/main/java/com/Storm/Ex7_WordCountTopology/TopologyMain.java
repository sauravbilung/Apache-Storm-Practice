package com.Storm.Ex7_WordCountTopology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReaderSpout());
		builder.setBolt("word-normalizer", new WordNormalizerBolt()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounterBolt(), 2).fieldsGrouping("word-normalizer", new Fields("word"));

		// Configuration
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("fileToRead", "/home/sauravbilung/Documents/Study/testFile3.txt");
		conf.put("dirToWrite", "/home/sauravbilung/Documents/Study/StormOutputs/");

		// Topology run
		LocalCluster cluster = new LocalCluster();
		try {
			cluster.submitTopology("Word-counter-Topology", conf, builder.createTopology());
			Thread.sleep(50000);
		} finally {
			cluster.shutdown();
		}

		System.out.println("Program has ended !!!!");

	}

}
