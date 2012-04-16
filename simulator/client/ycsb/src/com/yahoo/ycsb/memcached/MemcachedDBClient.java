package com.yahoo.ycsb.memcached;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import com.yahoo.ycsb.DBException;
import net.rubyeye.xmemcached.exception.MemcachedException;

/**
* @author Hao Li
*
*/
public class MemcachedDBClient extends MemcachedDB {
	private net.rubyeye.xmemcached.MemcachedClient mclient;
	
	public MemcachedDBClient(net.rubyeye.xmemcached.MemcachedClient mclient) {
		this.mclient = mclient;
	}
	/**
	 * Initialization
	 */
	public void init() throws DBException {
		Properties prop = super.getProperties();
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
			return mclient.set(key, 0, value);
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MemcachedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Delete from server
	 * @param key Key
	 */
	public boolean delete(String key) {
		return true;
	}
	
	public void cleanup() {
		try {
			mclient.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public net.rubyeye.xmemcached.MemcachedClient getClient() {
		return this.mclient;
	}
}



