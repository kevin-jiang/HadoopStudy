package com.logicsolutions.rpc;

import java.io.IOException;

public class MyBiz implements MyBizable{
	
	
	/* (non-Javadoc)
	 * @see com.logicsolutions.rpc.MyBizable#getProtocolVersion(java.lang.String, long)
	 */
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return 2345245l;
	}

	/* (non-Javadoc)
	 * @see com.logicsolutions.rpc.MyBizable#hello(java.lang.String)
	 */
	@Override
	public String hello(String name){
		System.out.println("我被调用了！");
		return "Hello " + name;
	}

}
