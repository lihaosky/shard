package com.yahoo.ycsb.experiment;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import com.yahoo.ycsb.DBException;

/**
 * Abstract memcached database
 */
public class Cache {
  public static int CACHE = 1;
  public static int BACKEND = 0;
  
  public int cacheSize;    // table size
  public int type;         // Type: cache or backend
  public int usedSize;
  
  public HashMap<String, Value> readCountMap;
  public HashMap<String, String> keyValueMap;
  public List<String> lruList;
  
  public Cache(int cacheSize, int type) {
    this.cacheSize = cacheSize;
    this.type = type;
    usedSize = 0;
  }
  
  public void init() {
    readCountMap = new HashMap<String, Value>();
    keyValueMap = new HashMap<String, String>();
    lruList = new LinkedList<String>();
  }
  
  public synchronized boolean read(String key) {
    String value = keyValueMap.get(key);
    if (value == null) {
      return false;
    } 
    Value v = readCountMap.get(key);
    if (v == null) {
      readCountMap.put(key, new Value(1));
    } else {
      v.increment();
    }
    if (type != Cache.BACKEND) {
      int index = lruList.indexOf(key);
      if (index != -1) {
        lruList.remove(index);
        lruList.add(key);
      }
    }
    return true;
  }
  
  public synchronized boolean insert(String key, String value) {
    keyValueMap.put(key, value);
    Value v = readCountMap.get(key);
    if (v == null) {
      readCountMap.put(key, new Value(0));
    }
    if (type != Cache.BACKEND) {
      usedSize++;
      if (usedSize > cacheSize) {
        String previousKey = lruList.remove(0);
        keyValueMap.remove(previousKey);
        usedSize--;
      } 
      lruList.add(key);
    }
    return true;
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
