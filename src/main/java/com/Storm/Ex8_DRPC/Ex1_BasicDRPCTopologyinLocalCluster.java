package com.Storm.Ex8_DRPC;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

public class Ex1_BasicDRPCTopologyinLocalCluster {

	public static void main(String[] args) throws InterruptedException {

		// AddTen is function name that we have given. This is the function we are applying
		// on the data.
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("AddTen");
		// adding a bolt and creating multiple instances of it.
		builder.addBolt(new AddTenBolt(), 3); 

		Config conf = new Config();
		
		// This is DRPC client which will talk to DRPC topology.
		LocalDRPC drpc = new LocalDRPC();
		// DRPC topology.
		LocalCluster cluster = new LocalCluster();
		// In builder.createLocalTopology() we are passing the drpc client that will be
		// using this topology. 
		cluster.submitTopology("AddTen", conf, builder.createLocalTopology(drpc));

		for (Integer number : new Integer[] { 10, 20, 30 }) {
			
			// drpc.execute is passed with the function name and the number on which that function
			// should be applied.
			System.out.println("Result :"+number + "+ 10 =" + drpc.execute("AddTen", number.toString()));
		}
		
		Thread.sleep(10000);
		cluster.shutdown();
		drpc.shutdown();
		System.out.println("Program has ended !!!");
	}

}
