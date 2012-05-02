package com.yahoo.ycsb.memcached;

import java.util.HashMap;
import java.util.Properties;
import com.yahoo.ycsb.DBException;

/**

 */
public abstract class MemcachedDB
{
	/**
	 * Map from key to read number
	 */
	private HashMap<Integer, Value> keyReadNumMap = new HashMap<Integer, Value>();
	
	/**
	 * Map from server to read number
	 */
	private HashMap<Integer, Value> serverReadNumMap = new HashMap<Integer, Value>();
	
	/**
	 * Map from server to average latency
	 */
	private HashMap<Integer, Value> serverLatencyMap = new HashMap<Integer, Value>();
	/**
	 * Properties for configuring this DB.
	 */
	Properties _p=new Properties();

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p)
	{
		_p=p;

	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties()
	{
		return _p; 
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
	}


	/**
	 * Read value from the 
	 * @param key The key to be deleted
	 * @param value The value read
	 * @return true if successful, false if not
	 */
	public abstract String read(String key);
	
	/**
	 * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key, overwriting any existing values with the same field name.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to write.
	 * @param values A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract boolean update(String key, String value);

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract boolean insert(String key, String value);

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract boolean delete(String key);
	
	public abstract int getAccessedServerID();
	
	/**
	 * Increment key read number
	 * @param keyNum
	 */
	public void incrementKeyReadNum(int keyNum) {
		Value v = keyReadNumMap.get(keyNum);
		
		if (v == null) {
			keyReadNumMap.put(keyNum, new Value(1));
		} else {
			v.increment();
		}
	}
	
	/**
	 * Increment server read number
	 * @param serverNum
	 */
	public void incrementServerReadNum(int serverNum) {
		Value v = serverReadNumMap.get(serverNum);
		
		if (v == null) {
			serverReadNumMap.put(serverNum, new Value(1));
		} else {
			v.increment();
		}
	}
	
	/**
	 * Increment server latency
	 * @param serverNum Server number
	 * @param latency Latency
	 */
	public void incrementServerLatency(int serverNum, int latency) {
		Value v = serverLatencyMap.get(serverNum);
		
		if (v == null) {
			serverLatencyMap.put(serverNum, new Value(latency));
		} else {
			v.incrementBy(latency);
		}
	}
	
	public HashMap<Integer, Value> getKeyReadNumMap() {
		return keyReadNumMap;
	}
	
	public HashMap<Integer, Value> getServerReadNumMap() {
		return serverReadNumMap;
	}
	
	public HashMap<Integer, Value> getServerLatencyMap() {
		return serverLatencyMap;
	}
}

class Value {
	private int value;
	
	public Value(int value) {
		this.value = value;
	}
	
	public void increment() {
		value++;
	}
	
	public void incrementBy(int value) {
		this.value += value;
	}
	
	public int getValue() {
		return value;
	}
}
