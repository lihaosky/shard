package com.yahoo.ycsb.util;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;

import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;

public class LoadGenerator {
	public static void usage() {
		System.out.println("Usage: java com.yahoo.ycsb.util.LoadGenerator [options]");
		System.out.println("Options:");
		System.out.println("-r: read proportion between 0 and 1");
		System.out.println("-w: write proportion between 0 and 1");
		System.out.println("-u: update proportion between 0 and 1");
		System.out.println("-k: key length. Default: 128");
		System.out.println("-v: value length. Default: 1024");
		System.out.println("-d: load distribution, uniform, zipfian or latest. Default: zipfian");
		System.out.println("-c: operation count");
		System.out.println("-o: output file path");
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {
		double readProp = 0;                //Read proportion
		double writeProp = 0;               //Write proportion
		double updateProp = 0;              //Update proportion
		String distribution = "zipfian";     //Distribution: uniform, zipfian or latest
		int operationCount = -1;             //Number of opertions
		int keyLength = 128;                 //Key length
		int valueLength = 1024;              //Value length
		String outputFilePath = "load.txt";   //Output file path
		
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-r")) {
				i++;
				readProp = Double.parseDouble(args[i]);
			} else if (args[i].equals("-w")) {
				i++;
				writeProp = Double.parseDouble(args[i]);
			} else if (args[i].equals("-u")) {
				i++;
				updateProp =Double.parseDouble(args[i]);
			} else if (args[i].equals("-d")) {
				i++;
				distribution = args[i];
			} else if (args[i].equals("-c")) {
				i++;
				operationCount = Integer.parseInt(args[i]);
			} else if (args[i].equals("-o")) {
				i++;
				outputFilePath = args[i];
			} else if (args[i].equals("-k")) {
				i++;
				keyLength = Integer.parseInt(args[i]);
			} else if (args[i].equals("-v")) {
				i++;
				valueLength = Integer.parseInt(args[i]);
			}
		}
		
		if (readProp > 1 || readProp < 0 || writeProp > 1 || writeProp < 0 || updateProp > 1 || updateProp < 0 || readProp + writeProp + updateProp != 1.0) {
			usage();
			System.exit(0);
		}
		if (operationCount <= 0 || keyLength <= 0 || valueLength <= 0) {
			usage();
			System.exit(0);
		}
		if (!(distribution.equals("zipfian") || distribution.equals("uniform") || distribution.equals("latest"))) {
			usage();
			System.exit(0);
		}
		
		LoadSummary ls = new LoadSummary(readProp, writeProp, updateProp, keyLength, valueLength, distribution, operationCount, outputFilePath);
		ls.printSummary();
		LoadWriter lw = new LoadWriter(ls);
		lw.writeLoad();
		
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(ls.outputFilePath));
		LoadSummary ls1 = (LoadSummary)ois.readObject();
		ls1.printSummary();
		do {
			Operation op;
			try {
			op = (Operation)ois.readObject();
			} catch (java.io.EOFException e) {
				break;
			}
			if (op == null) {
				break;
			}
			System.out.println("Operation is " + op.operation);
			System.out.println(op.key);
			if (lw.intKeyMap.containsValue(op.key) == false) {
				System.out.println("Error");
			}
		} while (true);
	}
}

/**
 * Load Summary
 * @author lihaosky
 *
 */
class LoadSummary implements Serializable {
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

class LoadWriter {
	private LoadSummary ls;
	private DiscreteGenerator operationchooser;
	private IntegerGenerator keychooser;
	private CounterGenerator transactioninsertkeysequence;
	public HashMap<Integer, String> intKeyMap;
	private boolean isFirst = true;
	
	public LoadWriter(LoadSummary ls) {
		this.ls = ls;
		operationchooser=new DiscreteGenerator();
		if (ls.readProp > 0) {
			operationchooser.addValue(ls.readProp, "READ");
		} 
		if (ls.writeProp > 0) {
			operationchooser.addValue(ls.writeProp, "INSERT");
		}
		if (ls.updateProp > 0) {
			operationchooser.addValue(ls.updateProp, "UPDATE");
		}
		
		transactioninsertkeysequence=new CounterGenerator(0);
		
		if (ls.distribution.compareTo("uniform") == 0) {
			keychooser=new UniformIntegerGenerator(0,1);
		}
		if (ls.distribution.compareTo("zipfian") == 0) {
			int expectednewkeys=(int)(((double)ls.operationCount)*ls.writeProp * 2.0); //2 is fudge factor
			
			keychooser=new ScrambledZipfianGenerator(0 + expectednewkeys);
		}
		if (ls.distribution.compareTo("latest") == 0) {
			keychooser=new SkewedLatestGenerator(transactioninsertkeysequence);
		}
		intKeyMap = new HashMap<Integer, String>();
	}
	
	public void writeLoad() throws FileNotFoundException, IOException {
		ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(ls.outputFilePath)));
		//Write header to file
		oos.writeObject(ls);
	
		//Generate load and write to file
		for (int i = 0; i < ls.operationCount; i++) {
			String op = operationchooser.nextString();
			Operation operation = null;
			
			if (isFirst) {
				op = "INSERT";
				isFirst = false;
			}
			
			if (op.compareTo("READ") == 0) {
				operation = readOperation();
			}
			if (op.compareTo("UPDATE") == 0) {
				operation = updateOperation();
			}
			if (op.compareTo("INSERT") == 0) {
				operation = insertOperation();
			}
			oos.writeObject(operation);
		}
		
		oos.flush();
		oos.close();
	}
	
	private Operation readOperation() {
		int keynum;
		do {
			keynum=keychooser.nextInt();
			
		} while (keynum>transactioninsertkeysequence.lastInt());
		
		String key = null;
		while (key == null) {
			key = intKeyMap.get(keynum);
		}
		
		return new Operation(Operation.READ, key, "");
	}
	
	private Operation updateOperation() {
		int keynum;
		do {
			keynum=keychooser.nextInt();
			
		} while (keynum>transactioninsertkeysequence.lastInt());
		
		String key = null;
		while (key == null) {
			key = intKeyMap.get(keynum);
		}
		
		return new Operation(Operation.UPDATE, key, "");
	}
	
	private Operation insertOperation() {
		int keynum=transactioninsertkeysequence.nextInt();

		String key = Utils.ASCIIString(ls.keyLength);
		String value = Utils.ASCIIString(ls.valueLength);
		intKeyMap.put(keynum, key);
		
		return new Operation(Operation.INSERT, key, value);
	}
}
