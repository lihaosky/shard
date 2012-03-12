package com.yahoo.ycsb.shard;

import java.util.Properties;
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
	 * Initialization
	 */
	public void init() throws DBException {
		Properties prop = super.getProperties();
		String serverHost = prop.getProperty("sharddb.server.host");
		int serverPort = Integer.parseInt(prop.getProperty("sharddb.server.port"));
		ssc = new ShardServerConnection(serverHost, serverPort);
	}
	
	/**
	 * Read from server
	 * @param key Key
	 * @param value Results stored in value
	 */
	public boolean read(byte[] key, byte[] value) {
		int serverID = findServerID();
		ssc.read(computeReachTime(), key, value, serverID);
		return false;
	}

	/**
	 * Update to server
	 * @param key Key
	 * @param value Results stored in value
	 */
	public boolean update(byte[] key, byte[] value) {
		int serverID = findServerID();
		ssc.update(computeReachTime(), key, value, serverID);
		return false;
	}

	/**
	 * Insert to server
	 * @param Key key
	 * @param value Results stored in value
	 */
	public boolean insert(byte[] key, byte[] value) {
		int serverID = findServerID();
		ssc.insert(computeReachTime(), key, value, serverID);
		return false;
	}

	/**
	 * Delete from server
	 * @param key Key
	 */
	public boolean delete(byte[] key) {
		int serverID = findServerID();
		ssc.delete(computeReachTime(), key, serverID);
		return false;
	}
	
	/**
	 * Find the serverID to contact
	 * @return ServerID
	 */
	private int findServerID() {
		return 0;
	}
	
	/**
	 * Compute the reach time of server in milliseconds to epoch.
	 * @return Reach time of server
	 */
	private long computeReachTime() {
		return System.currentTimeMillis() + latency;
	}

}


