package com.yahoo.ycsb.shard;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;

public class Test {
	public static void main(String[] args) {
		try {
			MemcachedClient client= new XMemcachedClient();
			client.addServer("localhost", 12344);
			client.set("lihaosky", 0, "what'sup");
			String value = client.get("lihaosky");
			System.out.println(value);
			client.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (MemcachedException e) {
			e.printStackTrace();
		}
		
		/*ShardControllerConnection scc = new ShardControllerConnection("localhost", 4321);
		String s = Utils.ASCIIString(128);
		byte[] key = s.getBytes();
		long reachTime = System.currentTimeMillis() + 100;
		scc.fetchMap(key, reachTime);
		scc.close();
		System.out.println(s);
		System.out.println(reachTime);*/
	}
}