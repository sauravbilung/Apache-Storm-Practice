package com.Storm.Ex8_DRPC;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

public class Ex2_BasicDRPCTopologyinRemoteCluster {

	public static void main(String[] args)
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

		// AddTen is function name that we have given. This is the function we are
		// applying
		// on the data.
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("AddTen");
		// adding a bolt and creating multiple instances of it.
		builder.addBolt(new AddTenBolt(), 3);

		Config conf = new Config();
		List<String> drpcServers = new ArrayList<String>();
		// DRPC Server address
		drpcServers.add("localhost");
		// All the requests from the DRPC client will go here(or listened here)
		// The requests are then passed to the topology configured with it.
		conf.put(Config.DRPC_SERVERS, drpcServers);
		conf.put(Config.DRPC_PORT, 3772);

		// Unlike in local cluster the topology name should be same
		// as the TopologyBuilder name.
		StormSubmitter.submitTopology("AddTen", conf, builder.createRemoteTopology());
	}

}
