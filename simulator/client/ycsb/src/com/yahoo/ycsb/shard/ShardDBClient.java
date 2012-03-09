package com.yahoo.ycsb.shard;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import com.yahoo.ycsb.DBException;

/**
* Client to do operations
* Properties to set:
* shardb.server.host=""   //Server host
* shardb.server.port=""   //Server port
* shardb.controller.host="" //Controller host
* shardb.controller.port="" //Controller port
* @author Hao Li
*
*/
public class ShardDBClient extends ShardDB {
	private long latency;
	private ShardServerConnection ssc;
	
	/**
	 * Constructor
	 * @param latency Latency for this DB in millisecond
	 */
	public ShardDBClient(long latency) {
		this.latency = latency;
	}
	
	/**
	 * Inialization
	 */
	public void init() throws DBException {
		Properties prop = super.getProperties();
		String serverHost = prop.getProperty("sharddb.server.host");
		int serverPort = Integer.parseInt(prop.getProperty("sharddb.server.port"));
		ssc = new ShardServerConnection(serverHost, serverPort);
	}
	
	/**
	 * Read from server
	 */
	public boolean read(byte[] key, byte[] value) {
		long reachTime = System.currentTimeMillis() + latency;
		int serverID = findServerID();
		ssc.read(reachTime, key, value, serverID);
		return false;
	}

	/**
	 * 
	 */
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, String>> result) {
		return 0;
	}

	/**
	 * Update to server
	 */
	public boolean update(byte[] key, byte[] value) {
		long reachTime = System.currentTimeMillis() + latency;
		int serverID = findServerID();
		ssc.update(reachTime, key, value, serverID);
		return false;
	}

	/**
	 * Insert to server
	 */
	public boolean insert(byte[] key, byte[] value) {
		long reachTime = System.currentTimeMillis() + latency;
		int serverID = findServerID();
		ssc.insert(reachTime, key, value, serverID);
		return false;
	}

	/**
	 * Delete from server
	 */
	public boolean delete(byte[] key) {
		long reachTime = System.currentTimeMillis() + latency;
		int serverID = findServerID();
		ssc.delete(reachTime, key, serverID);
		return false;
	}
	
	private int findServerID() {
		return 0;
	}

}


