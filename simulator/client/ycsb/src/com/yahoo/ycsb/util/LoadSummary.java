package com.yahoo.ycsb.util;

import java.io.Serializable;

/**
 * Load Summary
 * @author lihaosky
 *
 */
public class LoadSummary implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Read rate
	 */
	public double readProp;
	/**
	 * Write rate
	 */
	public double writeProp;
	/**
	 * Update rate
	 */
	public double updateProp;
	/**
	 * Key length
	 */
	public int keyLength;
	/**
	 * Value length
	 */
	public int valueLength;
	/**
	 * Distribution
	 */
	public String distribution;
	/**
	 * Operation number
	 */
	public int operationCount;
	/**
	 * Output file path
	 */
	public String outputFilePath;
	
	/**
	 * Constructor
	 * @param readProp
	 * @param writeProp
	 * @param updateProp
	 * @param distribution
	 * @param operationCount
	 */
	public LoadSummary(double readProp, double writeProp, double updateProp, int keyLength, int valueLength, String distribution, int operationCount, String outputFilePath) {
		this.readProp = readProp;
		this.writeProp = writeProp;
		this.updateProp = updateProp;
		this.distribution = distribution;
		this.operationCount = operationCount;
		this.keyLength = keyLength;
		this.valueLength = valueLength;
		this.outputFilePath = outputFilePath;
	}
	
	/**
	 * Print summary
	 */
	public void printSummary() {
		System.out.println("Totally " + operationCount + " operations");
		System.out.println("Read rate: " + readProp);
		System.out.println("Write rate: " + writeProp);
		System.out.println("Update rate: " + updateProp);
		System.out.println("Key length is " + keyLength);
		System.out.println("Value length is " + valueLength);
		System.out.println("Distribution: " + distribution);
	}
}
