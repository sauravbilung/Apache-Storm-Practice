package com.Storm.Ex8_DRPC;

import java.util.Map;

import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

public class DRPCClientMain {

	public static void main(String[] args) throws DRPCExecutionException, AuthorizationException, TException {

		@SuppressWarnings("rawtypes")
		Map conf = Utils.readStormConfig();
		// This will communicate with DRPC server.
		DRPCClient client = new DRPCClient(conf, "localhost", 3772);

		for (Integer number : new Integer[] { 10, 20, 30 }) {
			System.out.println("Result :" + number + "+ 10 =" + client.execute("AddTen", number.toString()));
		}
		
		client.close();
	}

}
