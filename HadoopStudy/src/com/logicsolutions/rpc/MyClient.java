package com.logicsolutions.rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		MyBizable myBiz = (MyBizable) RPC.waitForProxy(MyBizable.class, 2345245L, new InetSocketAddress(MyServer.bindAddress, MyServer.port), new Configuration());
		String result = myBiz.hello("Kevin");
		System.out.println(result);
		RPC.stopProxy(myBiz);
		
	}

}
