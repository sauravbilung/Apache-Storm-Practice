package com.Storm.Ex10_Storm_Kafka_Integration;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

@SuppressWarnings("deprecation")
public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		// Spout configuration
		String zkConnectionString = "localhost:2181";
		BrokerHosts zkHosts = new ZkHosts(zkConnectionString);
		String kafkaTopic = "storm-test-topic";
		String zkRoot = "/kafka";
		String clientId = "storm-consumer";
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, kafkaTopic, zkRoot, clientId);
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		// Topology creation
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("events-from-kafka", kafkaSpout, 4);
		builder.setBolt("split", new SplitLineBolt(), 4).shuffleGrouping("events-from-kafka");
		builder.setBolt("count", new WordCountBolt(), 1).fieldsGrouping("split", new Fields("word"));

		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(3);
		conf.put("dirToWrite", "/home/sauravbilung/Documents/Study/StormOutputs/");

		// Creating a cluster and submitting the topology
		LocalCluster cluster = new LocalCluster();
		try {
			cluster.submitTopology("word-count", conf, builder.createTopology());
			Thread.sleep(50000);
		} finally {
			cluster.shutdown();
		}

		System.out.println("Program has ended !!!");

	}
}
