package com.logicsolutions.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class MyServer {

	static final String bindAddress = "localhost";
	static final int port = 12345;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		
		Server server = RPC.getServer(new MyBiz(), bindAddress, port, new Configuration());
		server.start();

	}

}
