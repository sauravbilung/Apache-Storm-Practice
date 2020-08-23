package com.Storm.Ex5_RemoteTopology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import com.Storm.Ex1_HelloWorldTopology.Bolt;
import com.Storm.Ex1_HelloWorldTopology.Spout;

/*
 * The spout and bolt is same as in HelloWorld Topology package.
*/

public class TopologyMain {
	public static void main(String[] args)
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

		// Build Topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("My-First-spout", new Spout());
		builder.setBolt("My-First-bolt", new Bolt()).shuffleGrouping("My-First-spout");

		// Configuration
		Config conf = new Config();
		conf.setDebug(true); // set to true to see the logs in the console

		// Submit topology to cluster
		StormSubmitter.submitTopology("My-First-remote-topology", conf, builder.createTopology());
	}
}
