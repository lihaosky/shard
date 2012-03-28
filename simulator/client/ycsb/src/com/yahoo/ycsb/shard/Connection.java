package com.yahoo.ycsb.shard;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class Connection {
	/**
	 * Socket
	 */
	protected Socket s;
	/**
	 * Data output stream
	 */
	protected DataOutputStream dos;
	/**
	 * Data input stream
	 */
	protected DataInputStream dis;
	
	/**
	 * Constructor
	 * @param hostname Host name
	 * @param port Port number
	 */
	public Connection(String hostname, int port) {
		try {
			s = new Socket(hostname, port);
			dis = new DataInputStream(s.getInputStream());
			dos = new DataOutputStream(s.getOutputStream());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		try {
			dis.close();
			dos.close();
			s.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
