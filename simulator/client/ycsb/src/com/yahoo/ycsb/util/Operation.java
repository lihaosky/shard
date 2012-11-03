package com.yahoo.ycsb.util;

import java.io.Serializable;

/**
 * Operations to be written to file
 * @author lihaosky
 *
 */
public class Operation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2848358483519543300L;
	public static String INSERT = "insert";
	public static String UPDATE = "update";
	public static String READ = "read";
	/**
	 * Operation
	 */
	public String operation;
	/**
	 * Key
	 */
	public String key;
	/**
	 * Value
	 */
	public String value;
	
	public Operation(String operation, String key, String value) {
		this.operation = operation;
		this.key = key;
		this.value = value;
	}
}
