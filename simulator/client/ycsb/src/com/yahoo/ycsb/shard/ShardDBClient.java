package com.yahoo.ycsb.shard;

import java.util.HashMap;
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
	private long serverLatency;
	private long controllerLatency;
	private ShardServerConnection ssc;
	private ShardControllerConnection scc;
	private HashMap<byte[], Integer> keyServerMap;
	/**
	 * Constructor
	 * @param latency Latency for this DB in millisecond
	 */
	public ShardDBClient(long serverLatency, long controllerLatency) {
		this.serverLatency = serverLatency;
		this.controllerLatency = controllerLatency;
	}
	
	/**
	 * Initialization
	 */
	public void init() throws DBException {
		Properties prop = super.getProperties();
		
		/**
		 * Construct connection to server
		 */
		String serverHost = prop.getProperty("sharddb.server.host");
		int serverPort = Integer.parseInt(prop.getProperty("sharddb.server.port"));
		ssc = new ShardServerConnection(serverHost, serverPort);
		
		/**
		 * Construct connection controller
		 */
		String controllerHost = prop.getProperty("sharddb.controller.host");
		int controllerPort = Integer.parseInt(prop.getProperty("sharddb.controller.port"));
		scc = new ShardControllerConnection(controllerHost, controllerPort);
		
		/**
		 * Construct the mapping from key to server ID
		 */
		keyServerMap = new HashMap<byte[], Integer>();
	}
	
	/**
	 * Read from server
	 * @param key Key
	 * @param value Results stored in value
	 */
	public boolean read(byte[] key, byte[] value) {
		int serverID = findServerID(key);
		ssc.read(computeServerReachTime(), key, value, serverID);
		return false;
	}

	/**
	 * Update to server
	 * @param key Key
	 * @param value Results stored in value
	 */
	public boolean update(byte[] key, byte[] value) {
		int serverID = findServerID(key);
		ssc.update(computeServerReachTime(), key, value, serverID);
		return false;
	}

	/**
	 * Insert to server
	 * @param Key key
	 * @param value Results stored in value
	 */
	public boolean insert(byte[] key, byte[] value) {
		int serverID = findServerID(key);
		ssc.insert(computeServerReachTime(), key, value, serverID);
		return false;
	}

	/**
	 * Delete from server
	 * @param key Key
	 */
	public boolean delete(byte[] key) {
		int serverID = findServerID(key);
		ssc.delete(computeServerReachTime(), key, serverID);
		return false;
	}
	
	
	/**
	 * Compute the reach time of server in milliseconds to epoch.
	 * @return Reach time to server
	 */
	private long computeServerReachTime() {
		return System.currentTimeMillis() + serverLatency;
	}

	/**
	 * Compute the reach time of controller in milliseconds to epoch
	 * @return Reach time to controller
	 */
	private long computeControllerReachTime() {
		return System.currentTimeMillis() + controllerLatency;
	}
	
	/**
	 * Find the server ID according to the key
	 * @param key Key
	 * @return Server ID for the key
	 */
	private int findServerID(byte[] key) {
		Object serverID = keyServerMap.get(key);
		
		/**
		 * Need to fetch mapping from controller
		 */
		if (serverID == null) {
			keyServerMap = scc.fetchMap(key, computeControllerReachTime());
			return keyServerMap.get(key);
		} else {
			return (Integer)serverID;
		}
	}
	
	/**
	 * close connections...
	 */
	public void cleanup() {
		
	}
}


