package com.yahoo.ycsb.memcached;

import java.awt.List;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Measurement {
	private static final HashMap<Integer, Value> keyReadNumMap = new HashMap<Integer, Value>();
	private static final HashMap<Integer, Value> serverReadNumMap = new HashMap<Integer, Value>();
	private static final HashMap<Integer, Value> serverLatencyMap = new HashMap<Integer, Value>();
	private static final Object lock1 = new Object();
	private static final Object lock2 = new Object();
	private static final Object lock3 = new Object();
	
	public static void incrementKeyReadNum(int keyNum) {
		synchronized (lock1) {
			Value v = keyReadNumMap.get(keyNum);
			
			if (v == null) {
				keyReadNumMap.put(keyNum, new Value(1));
			} else {
				v.increment();
			}
		}
	}
	
	public static void incrementServerReadNum(int serverNum) {
		synchronized (lock2) {
			Value v = serverReadNumMap.get(serverNum);
			
			if (v == null) {
				serverReadNumMap.put(serverNum, new Value(1));
			} else {
				v.increment();
			}
		}
	}
	
	public static void incrementServerLatency(int serverNum, int latency) {
		synchronized (lock3) {
			Value v = serverLatencyMap.get(serverNum);
			
			if (v == null) {
				serverLatencyMap.put(serverNum, new Value(latency));
			} else {
				v.incrementBy(latency);
			}
		}
	}
	
	public static void report() {
		PrintWriter pw = null;
		PrintWriter pw1 = null;
		try {
			pw = new PrintWriter("Read.txt");
			pw1 = new PrintWriter("Server.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		
		System.out.println("=======================================================");
		System.out.println("================Read number per server=================");
		for (Integer i : serverReadNumMap.keySet()) {
			System.out.println(i + ": " + serverReadNumMap.get(i).getValue());
		}
		
		System.out.println("=======================================================");
		System.out.println("================Server average latency=================");
		for (Integer i : serverLatencyMap.keySet()) {
			System.out.println(i + ": " + (double)serverLatencyMap.get(i).getValue() / serverReadNumMap.get(i).getValue()); 
		}
		
		ArrayList<Map.Entry<Integer, Value>> al = sortMap(keyReadNumMap);
		for (int i = al.size() - 1, j = 1; i >= 0; i--, j++) {
			Map.Entry<Integer, Value> entry = al.get(i);
			pw.println(j + "\t" + entry.getValue().getValue());
		}
		
		pw.close();
		
		ArrayList<Map.Entry<Integer, Value>> al1 = sortMap(serverReadNumMap);
		for (int i = al1.size() - 1, j = 1; i >= 0; i--, j++) {
			Map.Entry<Integer, Value> entry = al1.get(i);
			pw1.println(j + "\t" + entry.getValue().getValue() + "\t" + (double)serverLatencyMap.get(entry.getKey()).getValue() / serverReadNumMap.get(entry.getKey()).getValue());
		}
		
		pw1.close();
	}
	
	private static ArrayList<Map.Entry<Integer, Value>> sortMap(HashMap<Integer,Value> map) {
		ArrayList<Map.Entry<Integer, Value>> al = new ArrayList<Map.Entry<Integer, Value>>(map.entrySet());
		Collections.sort(al, new Comparator<Map.Entry<Integer, Value>>() {
			public int compare(Entry<Integer, Value> o1,
					Entry<Integer, Value> o2) {
				
				return ((Comparable<Integer>)(o1.getValue().getValue())).compareTo(o2.getValue().getValue());
			}
			
		});
		
		return al;
	}
}


class Value {
	private int value;
	
	public Value(int value) {
		this.value = value;
	}
	
	public void increment() {
		value++;
	}
	
	public void incrementBy(int value) {
		this.value += value;
	}
	
	public int getValue() {
		return value;
	}
}