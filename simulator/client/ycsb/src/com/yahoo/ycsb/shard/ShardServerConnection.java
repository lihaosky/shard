package com.yahoo.ycsb.shard;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;

/**
 * Handle operation to remote db using socket
 * @author lihao
 *
 */
public class ShardServerConnection {
	/**
	 * Socket to server
	 */
	private Socket s;
	/**
	 * Data output stream for the socket 
	 */
	private DataOutputStream dos;
	/**
	 * Total packet length:      4 bytes int
	 * Time to arrive at server: 8 bytes long
	 * Command type:             4 bytes int
	 * Source IP address:        4 bytes int
	 * Server ID:                4 bytes int 
	 */
	private int headerLength = 4 + 8 + 4 + 4 + 4;
	
	public ShardServerConnection(String hostname, int port) {
		try {
			s = new Socket(hostname, port);
			dos = new DataOutputStream(s.getOutputStream());
		} catch (UnknownHostException e) {
			System.err.println("Unknownhost Exception!");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("IO Exception in connecting to server!");
			e.printStackTrace();
		}
	}
	
	/**
	 * Insert key value to server
	 * @param time Time to arrive in server
	 * @param key Key to be inserted
	 * @param value Value to be inserted
	 * @return true if successful, false if not
	 */
	public boolean insert(long time, byte[] key, byte[] value, int serverID) {
		int totalLength =  headerLength + key.length + value.length;
		//Source to be done
		writeHeader(totalLength, time, CommandType.INSERT, 0, serverID);
		try {
			dos.write(key);
			dos.write(value);
		} catch (IOException e) {
			System.err.println("IO Exception in insert!");
			e.printStackTrace();
		}
		
		return true;
	}
	
	/**
	 * Delete key value from server, if ever used
	 * @param time Time to arrive in server
	 * @param key Key to be deleted
	 * @return true if successful, false if not
	 */
	public boolean delete(long time, byte[] key, int serverID) {
		int totalLength = headerLength + key.length;
		//Source to be done
		writeHeader(totalLength, time, CommandType.DELETE, 0, serverID);
		try {
			dos.write(key);
		} catch (IOException e) {
			System.err.println("IO Exception in delete!");
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	 * Read key value from server
	 * @param time Time to arrive in server
	 * @param key Key to read
	 * @param value Value read
	 * @return Value 
	 */
	public boolean read(long time, byte[] key, byte[] value, int serverID) {
		int totalLength = headerLength + key.length;
		//Source to be done
		writeHeader(totalLength, time, CommandType.READ, 0, serverID);
		try {
			dos.write(key);
		} catch (IOException e) {
			System.err.println("IO Exception in read!");
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	 * Update key value in server
	 * @param time Time to arrive in server
	 * @param key Key to be updated
	 * @param value new Value
	 * @return true if successful, false if not
	 */
	public boolean update(long time, byte[] key, byte[] value, int serverID) {
		int totalLength = headerLength + key.length + value.length;
		//Source to be done
		writeHeader(totalLength, time, CommandType.UPDATE, 0, serverID);
		try {
			dos.write(key);
			dos.write(value);
		} catch (IOException e) {
			System.err.println("IO Exception in update!");
			e.printStackTrace();
		}
		return true;
	}
	
	private void writeHeader(int totalLength, long time, int command, int source, int serverID) {
		try {
			dos.writeInt(totalLength);
			dos.writeLong(time);
			dos.writeInt(CommandType.INSERT);
			//Write the IP of client, to be done
			dos.writeInt(serverID);
		} catch (IOException e) {
			System.err.println("IO Exception in writing header!");
			e.printStackTrace();
		}
	}
	
	public void close() {
		try {
			dos.close();
			s.close();
		} catch (IOException e) {
			System.err.println("IO Exception in closing socket!");
			e.printStackTrace();
		}
	}
}
