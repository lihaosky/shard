package com.yahoo.ycsb.memcached;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.Iterator;;

public class Measurement {
	public static void report(Vector<MemcachedDB> dbs) {
		HashMap<Integer, Value> keyReadNumMap = null;
		/**
		 * Aggregate all the db key read number list
		 */
		for (MemcachedDB db : dbs) {
			HashMap<Integer, Value> tmpMap = db.getKeyReadNumMap();
			
			if (keyReadNumMap == null) {
				keyReadNumMap = tmpMap;
			} else {
				Iterator<Map.Entry<Integer, Value>> it = tmpMap.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<Integer, Value> entry = it.next();
					
					Value v = keyReadNumMap.get(entry.getKey());
					if (v == null) {
						keyReadNumMap.put(entry.getKey(), entry.getValue());
					} else {
						keyReadNumMap.get(entry.getKey()).incrementBy(entry.getValue().getValue());
					}
				}
			}
		}
		
		/**
		 * 
		 */
		ArrayList<Map.Entry<Integer, Value>> keyReadNumList = sortMapByValue(keyReadNumMap);
	
		HashMap<Integer, Value> serverReadNumMap = null;
		/**
		 * Aggregate all the db key read number list
		 */
		int p = 0;
		for (MemcachedDB db : dbs) {
			HashMap<Integer, Value> tmpMap = db.getServerReadNumMap();
			
			if (serverReadNumMap == null) {
				serverReadNumMap = tmpMap;
			} else {
				Iterator<Map.Entry<Integer, Value>> it = tmpMap.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<Integer, Value> entry = it.next();
					
					Value v = serverReadNumMap.get(entry.getKey());
					if (v == null) {
						serverReadNumMap.put(entry.getKey(), entry.getValue());
					} else {
						serverReadNumMap.get(entry.getKey()).incrementBy(entry.getValue().getValue());
					}
				}
			}
		}
		ArrayList<Map.Entry<Integer, Value>> serverReadNumList = sortMapByValue(serverReadNumMap);
		
		
		HashMap<Integer, Value> serverLatencyMap = null;
		/**
		 * Aggregate all the db key read number list
		 */
		for (MemcachedDB db : dbs) {
			HashMap<Integer, Value> tmpMap = db.getServerLatencyMap();
			
			if (serverLatencyMap == null) {
				serverLatencyMap = tmpMap;
			} else {
				Iterator<Map.Entry<Integer, Value>> it = tmpMap.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<Integer, Value> entry = it.next();
					
					Value v = serverLatencyMap.get(entry.getKey());
					if (v == null) {
						serverLatencyMap.put(entry.getKey(), entry.getValue());
					} else {
						serverLatencyMap.get(entry.getKey()).incrementBy(entry.getValue().getValue());
					}
				}
			}
		}
		
		PrintWriter pw = null;
		PrintWriter pw1 = null;
		try {
			pw = new PrintWriter("Read.txt");
			pw1 = new PrintWriter("Server.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		
		for (int i = keyReadNumList.size() - 1, j = 1; i >= 0; i--, j++) {
			Map.Entry<Integer, Value> entry = keyReadNumList.get(i);
			pw.println(j + "\t" + entry.getValue().getValue());
		}
		pw.close();
		
		
		for (int i = serverReadNumList.size() - 1, j = 1; i >= 0; i--, j++) {
			Map.Entry<Integer, Value> entry = serverReadNumList.get(i);
			pw1.println(j + "\t" + entry.getValue().getValue() + "\t" + (double)serverLatencyMap.get(entry.getKey()).getValue() / entry.getValue().getValue());
		}
		
		pw1.close();
	}
	
	private static ArrayList<Map.Entry<Integer, Value>> sortMapByValue(HashMap<Integer,Value> map) {
		ArrayList<Map.Entry<Integer, Value>> al = new ArrayList<Map.Entry<Integer, Value>>(map.entrySet());
		Collections.sort(al, new Comparator<Map.Entry<Integer, Value>>() {
			public int compare(Entry<Integer, Value> o1, Entry<Integer, Value> o2) {
				return ((Comparable<Integer>)(o1.getValue().getValue())).compareTo(o2.getValue().getValue());
			}
			
		});
		
		return al;
	}
	
}