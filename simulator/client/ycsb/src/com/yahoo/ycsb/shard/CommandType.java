package com.yahoo.ycsb.shard;

/**
 * Command type in messages
 * @author lihao
 *
 */
public class CommandType {
	/**
	 * Read from server db
	 */
	public static final int READ = 0; 
	/**
	 * Insert into server db
	 */
	public static final int INSERT = 1;
	/**
	 * Update server db
	 */
	public static final int UPDATE = 2;
	/**
	 * Delete from server db
	 */
	public static final int DELETE = 3;
	/**
	 * Request mapping from controller
	 */
	public static final int REQUESTMAP = 4;
	
}
