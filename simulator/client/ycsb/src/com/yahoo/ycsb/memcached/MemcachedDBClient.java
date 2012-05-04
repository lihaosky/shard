package com.yahoo.ycsb.memcached;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import com.yahoo.ycsb.DBException;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.impl.ArrayMemcachedSessionLocator;

/**
* @author Hao Li
*
*/
public class MemcachedDBClient extends MemcachedDB {
	private Vector<net.rubyeye.xmemcached.MemcachedClient> mclients;
	private HashMap<String, Integer> hostClientMap;
	private volatile ConcurrentHashMap<String, Vector<String>> keyServerMap;
	private int lastServerID;
	private MapFetchThread mft;
	/**
	 * Hashing key to server
	 */
	private ArrayMemcachedSessionLocator keyServerLocator = new ArrayMemcachedSessionLocator();
	
	public MemcachedDBClient(Vector<net.rubyeye.xmemcached.MemcachedClient> mclients, HashMap<String, Integer> hostClientMap, String controllerHost, int controllerPort) {
		this.mclients = mclients;
		this.hostClientMap = hostClientMap;
		keyServerMap = new ConcurrentHashMap<String, Vector<String>>();
		mft = new MapFetchThread(keyServerMap, controllerHost, controllerPort);
		mft.start();
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
		int serverID;
		Vector<String> hosts = keyServerMap.get(key);
		
		/**
		 * Find the server
		 * If not in keyServerMap, find the insertion server
		 * If in, find a random one in hosts
		 */
		if (hosts == null) {
			serverID = findServerByKey(key);
		} else {
			serverID = hostClientMap.get(hosts.get(new Random().nextInt(hosts.size())));
		}
		lastServerID = serverID;
		
		try {
			value = mclients.get(serverID).get(key);
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
		/**
		 * Find the insertion server by default hashing
		 */
		int serverID = findServerByKey(key);
		lastServerID = serverID;
		
		try {
			return mclients.get(serverID).set(key, 0, value);
		} catch (TimeoutException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (MemcachedException e) {
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
	
	/**
	 * Stop clients and fetching thread
	 */
	public void cleanup() {
		try {
			for (net.rubyeye.xmemcached.MemcachedClient mclient : mclients) {
				mclient.shutdown();
			}
			mft.halt();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Find server by key
	 * Used when the mapping is unknown
	 * @param key Key
	 * @return ServerID
	 */
	public int findServerByKey(String key) {
		return (int) keyServerLocator.getHash(mclients.size(), key);
	}
	
	/**
	 * Return last accessed serverID
	 */
	public int getAccessedServerID() {
		return lastServerID;
	}
}

/**
 * Thread to pull the map very 5 seconds
 * @author lihao
 *
 */
class MapFetchThread extends Thread {
	/**
	 * Ker server map
	 * Key --> Server ip list
	 */
	private ConcurrentHashMap<String, Vector<String>> keyServerMap;
	/**
	 * Controller host name
	 */
	private String controllerHost;
	/**
	 * Controller port number
	 */
	private int controllerPort;
	/**
	 * Should the thread be stopped
	 */
	private boolean stop = false;
	
	/**
	 * Constructor
	 * @param keyServerMap Key server map from DB
	 * @param controllerHost Controller host name
	 * @param controllerPort Controller port number
	 */
	public MapFetchThread(ConcurrentHashMap<String, Vector<String>> keyServerMap, String controllerHost, int controllerPort) {
		this.keyServerMap = keyServerMap;
		this.controllerHost = controllerHost;
		this.controllerPort = controllerPort;
	}
	
	public void run() {
		Socket s = null;
		DataOutputStream dos = null;
		BufferedReader br = null;
		
		try {
			s = new Socket(controllerHost, controllerPort);
			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			dos = new DataOutputStream(s.getOutputStream());
		} catch (UnknownHostException e) {
			System.err.println("Unknowned controller host!");
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			System.err.println("IO error connecting to controller!");
			e.printStackTrace();
			System.exit(1);
		}
		
		while (!stop) {
			try {
				//Send request
				dos.write("2:\r\n".getBytes());
				
				//Read mapping
				String line = null;
				while ((line = br.readLine()) != null) {
					if (line.equals("")) {
						break;
					}
					String[] tokens = line.split(":");
					String[] ips = tokens[1].split(",");
					
					Vector<String> IPVector = new Vector<String>();
					for (String ip : ips) {
						IPVector.add(ip);
					}
					
					keyServerMap.put(tokens[0], IPVector);
				}
			} catch (IOException e) {
				System.err.println("Error in writing to controller!");
				e.printStackTrace();
				System.exit(1);
			}
			
			/**
			 * Wait for 5 seconds before next fetching request
			 */
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	/**
	 * Stop thread
	 */
	public void halt() {
		stop = true;
	}
}


