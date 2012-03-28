package com.yahoo.ycsb.shard;

import com.yahoo.ycsb.Utils;

public class Test {
	public static void main(String[] args) {
		ShardControllerConnection scc = new ShardControllerConnection("localhost", 4321);
		String s = Utils.ASCIIString(128);
		byte[] key = s.getBytes();
		long reachTime = System.currentTimeMillis() + 100;
		scc.fetchMap(key, reachTime);
		scc.close();
		System.out.println(s);
		System.out.println(reachTime);
	}
}