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
		HashMap<byte[], Integer> map = new HashMap<byte[], Integer>();
		
		try {
			dos.writeInt(headerLength);
			dos.writeLong(reachTime);
			dos.writeInt(CommandType.REQUESTMAP);
			dos.write(key);
			
			/**
			 * First read total key number and then read each key and corresponding server number
			 */
			int totalKeyNumber = dis.readInt();
			for (int i = 0; i < totalKeyNumber; i++) {
				byte[] b = new byte[ShardWorkload.fieldlength];
				dis.read(b, 0, ShardWorkload.fieldlength);
				int serverNumber = dis.readInt();
				map.put(b, serverNumber);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return map;
	}
}
