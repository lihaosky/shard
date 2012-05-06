package com.yahoo.ycsb.memcached;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.yahoo.ycsb.DBException;
import net.rubyeye.xmemcached.exception.MemcachedException;

/**
* @author Hao Li
*
*/
public class MemcachedDBClient extends MemcachedDB {
	/**
	 * Hashing key to server
	 */
	private net.rubyeye.xmemcached.MemcachedClient mclient;
	
	public MemcachedDBClient(net.rubyeye.xmemcached.MemcachedClient mclient) {
		this.mclient = mclient;
	}
	
	/**
	 * Initialization
	 */
	public void init() throws DBException {
	}
	
	/**
	 * Read from server
	 * @param key Key
	 */
	public String read(String key) {
		String value = null;
		try {
			value = mclient.get(key);
		} catch (TimeoutException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (MemcachedException e) {
			e.printStackTrace();
		}
		return value;
	}

	/**
	 * Update to server
	 * @param key Key
	 * @param value Results stored in value
	 */
	public boolean update(String key, String value) {
		return true;
	}

	/**
	 * Insert to server
	 * @param Key key
	 * @param value Results stored in value
	 */
	public boolean insert(String key, String value) {
		try {
			mclient.set(key, 0, value);
		} catch (TimeoutException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (MemcachedException e) {
			e.printStackTrace();
		}
		
		return true;
	}

	/**
	 * Delete from server
	 * @param key Key
	 */
	public boolean delete(String key) {
		return true;
	}
	
	/**
	 * Stop clients and fetching thread
	 */
	public void cleanup() {
		try {
			mclient.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Return last accessed serverID
	 */

	public int getAccessedServerID() {
		return mclient.lastIndex();
	}
}


