package com.yahoo.ycsb.shard;

import java.io.IOException;
import java.util.HashMap;

/**
 * Handle operation to controller using socket
 * @author lihao
 *
 */
public class ShardControllerConnection extends Connection{
	
	/**
	 * Total packet length:      4 bytes int
	 * Time to arrive at server: 8 bytes long
	 * Command type:             4 bytes int
	 * Key:                      128 bytes
	 */
	private int headerLength = 4 + 8 + 4 + 128;
	
	/**
	 * Constructor
	 * @param hostname Controller host name
	 * @param port Controller port number
	 */
	public ShardControllerConnection(String hostname, int port) {
		super(hostname, port);
	}
	
	/**
	 * Fetch map from controller
	 * @param key Key to be contained the mapping
	 * @return Map from key to serverID
	 */
	public HashMap<byte[], Integer> fetchMap(byte[] key, long reachTime) {
		try {
			dos.writeInt(headerLength);
			dos.writeLong(reachTime);
			dos.writeInt(CommandType.REQUESTMAP);
			dos.write(key);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new HashMap<byte[], Integer>();
	}
}
