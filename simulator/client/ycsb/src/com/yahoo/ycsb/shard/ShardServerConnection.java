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
	private int headerLength = 4 + 8 + 4 + 4 + 4 + 4;
	
	public ShardServerConnection(String hostname, int port) {
		try {
			s = new Socket(hostname, port);
			dos = new DataOutputStream(s.getOutputStream());
		} catch (UnknownHostException e) {
			System.err.println("Unknownhost!");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("IO Exception!");
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
		writeHeader(totalLength, time, CommandType.INSERT, 0, serverID, key.length);
		
		
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
		
		try {
			dos.writeInt(totalLength);
			dos.writeLong(time);
			
		} catch (IOException e) {
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
		return true;
	}
	
	private void writeHeader(int totalLength, long time, int command, int source, int serverID, int keyLength) {
		try {
			dos.writeInt(totalLength);
			dos.writeLong(time);
			dos.writeInt(CommandType.INSERT);
			//Write the IP of client, to be done
			dos.writeInt(serverID);
			dos.writeInt(keyLength);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
