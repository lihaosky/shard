package com.yahoo.ycsb.shard;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import com.yahoo.ycsb.DBException;

/**
* Client to do operations
* Properties to set:
* shardb.server.host=""
* shardb.server.port=""
* shardb.controller.host=""
* shardb.controller.port=""
* @author Hao Li
*
*/
public class ShardDBClient extends ShardDB {
	/**
	 * Inialization
	 */
	public void init() throws DBException {
		
	}
	
	/**
	 * Read from server
	 */
	public boolean read(byte[] key, byte[] value) {
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
		return false;
	}

	/**
	 * Insert to server
	 */
	public boolean insert(byte[] key, byte[] value) {
		return false;
	}

	/**
	 * Delete from server
	 */
	public boolean delete(byte[] key) {
		return false;
	}
	

}


