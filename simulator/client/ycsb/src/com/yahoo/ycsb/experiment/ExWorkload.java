package com.yahoo.ycsb.experiment;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.ycsb.util.*;


public class ExWorkload
{
  private boolean isFirst = true;
  
  /**
   * Store int to byte key mapping. For reuse bytes.
   */
  private ConcurrentHashMap<Integer, String> IntKeyMap;
  
  private ConcurrentHashMap<String, Integer> keyIDMap;
  private ConcurrentHashMap<String, Value> keyCountMap;
  
  /**
   * The name of the property for the length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY="fieldlength";
  
  /**
   * The default length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY_DEFAULT="1048576";

    int fieldlength;
  
  /**
   * The name of the property for the length of a key in bytes
   */
  public static final String KEY_LENGTH_RROPERTY = "keylength";
  
  /**
   * The default length of a key in bytes
   */
  public static final String KEY_LENGTH_PROPERTY_DEFAULT = "128";
  
  int keylength;


  /*********************************************************
   * Configure the proportion of read, write, update, etc. *
   *********************************************************/
  /**
   * The name of the property for the proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY="readproportion";
  
  /**
   * The default proportion of transactions that are reads. 
   */
  public static final String READ_PROPORTION_PROPERTY_DEFAULT="0.80";

  double readProportion;
  /**
   * The name of the property for the proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY="updateproportion";
  
  double updateProportion;
  
  /**
   * The default proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT="0.00";
  /**
   * The name of the property for the proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY="insertproportion";
  
  double insertProportion;
  /**
   * The default proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY_DEFAULT="0.20";
  
  
  /***************************************************/
  
  
  /*******************************************************
   * Configure the distribution of request               *
   ******************************************************/
  /**
   * The name of the property for the the distribution of requests across the keyspace. Options are "uniform", "zipfian" and "latest"
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY="requestdistribution";
  
  /**
   * The default distribution of requests across the keyspace
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT="zipfian";
  /********************************************************/
  
  
  /**
   * The name of the property for the order to insert records. Options are "ordered" or "hashed"
   */
  public static final String INSERT_ORDER_PROPERTY="insertorder";
  
  /**
   * Default insert order.
   */
  public static final String INSERT_ORDER_PROPERTY_DEFAULT="hashed";
  
  
  IntegerGenerator keysequence;

  DiscreteGenerator operationchooser;

  IntegerGenerator keychooser;

  Generator fieldchooser;

  CounterGenerator transactioninsertkeysequence;
  
  IntegerGenerator scanlength;
  
  boolean orderedinserts;

  int recordcount;
  
  int objID = 0;
  
  /**
   * If operations are from file
   */
  boolean isFromFile = false;
  /**
   * Operations loaded from file
   */
  Vector<Operation> operations;
  /**
   * Indicated if a key is successfully inserted
   */
  ConcurrentHashMap<String, Integer> isInserted;
  /**
   * Next operation from vector
   */
  int seq = 0;
  /**
   * Total number of operations
   */
  int opcount = 0;
  /**
   * Read count
   */
  int readCount = 0;
  /**
   * Insert count
   */
  int insertCount = 0;
  
  /**
   * Initialize the scenario. 
   * Called once, in the main client thread, before any operations are started.
   */
  public void init(Properties p) throws WorkloadException
  {
    fieldlength=Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY,FIELD_LENGTH_PROPERTY_DEFAULT));
    keylength = Integer.parseInt(p.getProperty(KEY_LENGTH_RROPERTY, KEY_LENGTH_PROPERTY_DEFAULT));
    readProportion=Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY,READ_PROPORTION_PROPERTY_DEFAULT));
    updateProportion=Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY,UPDATE_PROPORTION_PROPERTY_DEFAULT));
    insertProportion=Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY,INSERT_PROPORTION_PROPERTY_DEFAULT));
    recordcount=Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY));
    String requestdistrib=p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
    int insertstart=Integer.parseInt(p.getProperty(Workload.INSERT_START_PROPERTY,Workload.INSERT_START_PROPERTY_DEFAULT));
    
    if (p.getProperty(INSERT_ORDER_PROPERTY,INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed")==0)
    {
      orderedinserts=false;
    }
    else
    {
      orderedinserts=true;
    }

    keysequence=new CounterGenerator(insertstart);
    operationchooser=new DiscreteGenerator();
    
    //Operation proportion
    if (readProportion>0)
    {
      operationchooser.addValue(readProportion,"READ");
    }

    if (updateProportion>0)
    {
      operationchooser.addValue(updateProportion,"UPDATE");
    }

    if (insertProportion>0)
    {
      operationchooser.addValue(insertProportion,"INSERT");
    }

    transactioninsertkeysequence=new CounterGenerator(recordcount);
    
    
    //Key distribution
    if (requestdistrib.compareTo("uniform")==0)
    {
      keychooser=new UniformIntegerGenerator(0,recordcount-1);
      System.out.println("Distribution is uniform");
    }
    else if (requestdistrib.compareTo("zipfian")==0)
    {
      //it does this by generating a random "next key" in part by taking the modulus over the number of keys
      //if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
      //so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
      //of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
      //plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
      //just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled zipfian generator
      
      int opcount=Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
      int expectednewkeys=(int)(((double)opcount)*insertProportion*2.0); //2 is fudge factor
      
      keychooser=new ScrambledZipfianGenerator(recordcount+expectednewkeys);
      System.out.println("Distribution is zipfian");
    }
    else if (requestdistrib.compareTo("latest")==0)
    {
      keychooser=new SkewedLatestGenerator(transactioninsertkeysequence);
      System.out.println("Distribution is latest");
    }
    else
    {
      throw new WorkloadException("Unknown distribution \""+requestdistrib+"\"");
    }
    
    IntKeyMap = new ConcurrentHashMap<Integer, String>();
    keyIDMap = new ConcurrentHashMap<String, Integer>();
    keyCountMap = new ConcurrentHashMap<String, Value>();
    
    printConfig();
  }

  
  private void printConfig() {
    System.out.println("Key length is " + this.keylength);
    System.out.println("Value length is " + this.fieldlength);
    System.out.println("Read proportion is " + this.readProportion);
    System.out.println("Update proportion is " + this.updateProportion);
    System.out.println("Insert proportion is " + this.insertProportion);
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client threads, this 
   * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
   * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
   * effects other than DB operations.
   */
  public boolean doTransaction(Cache[] db, Object threadstate)
  {
    /**
     * From file
     */
    if (isFromFile) {
      Operation op = nextOperation();
      if (op == null) {
        return false;
      }
      synchronized (this) {
        Value v = keyCountMap.get(op.key);
        if (v == null) {
          keyCountMap.put(op.key, new Value(1));
        } else {
          v.increment();
        }
      }
      synchronized (this) {
        Integer id = keyIDMap.get(op.key);
        if (id == null) {
          objID++;
          keyIDMap.put(op.key, objID);
        }
      }
      if (op.operation.compareTo(Operation.READ) == 0) {
        transactionRead(db, op);
        synchronized (this) {
          readCount++;
        }
      } else if (op.operation.compareTo(Operation.INSERT) == 0) {
        transactionInsert(db, op);
        synchronized (this) {
          insertCount++;
        }
      } else if (op.operation.compareTo(Operation.UPDATE) == 0) {
        transactionUpdate(db, op);
      }
    } else {
      String op=operationchooser.nextString();
      
      if (isFirst) {
        op = "INSERT";
        isFirst = false;
      }
      
      if (op.compareTo("READ")==0)
      {
        doTransactionRead(db);
        synchronized (this) {
          readCount++;
        }
      }
      else if (op.compareTo("UPDATE")==0)
      {
        doTransactionUpdate(db);
      }
      else if (op.compareTo("INSERT")==0)
      {
        doTransactionInsert(db);
        synchronized (this) {
          insertCount++;
        }
      }
    }
    return true;
  }

  /**
   * Read from db
   * @param db DB
   * @param op Operation
   */
  public void transactionRead(Cache[] db, Operation op) {
    int count = 0;
    // Wait until the key is successfully inserted
    while (!isInserted.containsKey(op.key)) {
      if (count == 10) {
        break;
      }
      count++;
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    int i;
    for (i = 0; i < db.length; i++) {
      if (db[i].read(op.key)) {
        break;
      }
    }
    for (int j = 0; j < i; j++) {
      db[j].insert(op.key, op.value);
    }
  }
  
  /**
   * Insert into DB
   * @param db DB
   * @param op Operation
   */
  public void transactionInsert(Cache[] db, Operation op) {
    Cache backend = db[db.length - 1];
    backend.insert(op.key, op.value);
    isInserted.put(op.key, 1);
  }
  
  public void transactionUpdate(Cache[] db, Operation op) {
    
  }
  /**
   * Read
   * @param db Database
   */
  public void doTransactionRead(Cache[] db)
  {
    
  }
  
  /**
   * Update
   * @param db Database
   */
  public void doTransactionUpdate(Cache[] db)
  {
  }

  /**
   * Insert
   * @param db Database
   */
  public void doTransactionInsert(Cache[] db)
  {
  }
  
  /**
   * Load operation from file
   * @param fileName File name
   * @return false if fails. Otherwise true
   */
  public boolean loadOperations(String fileName) {
    isFromFile = true;
    operations = new Vector<Operation>();
    isInserted = new ConcurrentHashMap<String, Integer>();
    ObjectInputStream ois;
    
    try {
      ois = new ObjectInputStream(new FileInputStream(fileName));
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
      return false;
    } catch (IOException e1) {
      e1.printStackTrace();
      return false;
    } 
    LoadSummary ld;
    try {
      ld = (LoadSummary)ois.readObject();
    } catch (IOException e1) {
      e1.printStackTrace();
      return false;
    } catch (ClassNotFoundException e1) {
      e1.printStackTrace();
      return false;
    }
    ld.printSummary();
    
    while (true) {
      try {
        Operation op = (Operation)ois.readObject();
        operations.add(op);
        opcount++;
      } catch (java.io.EOFException e) {
        break;
      } catch (IOException e) {
        e.printStackTrace();
        return false;
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        return false;
      }
    }
    return true;
  }
  
  /**
   * Next operation to perform
   * @return
   */
  synchronized public Operation nextOperation() {
    if (seq >= operations.size()) {
      return null;
    }
    return operations.get(seq++);
  }
  
  public void printSummary() {
    System.out.println("Read count is " + readCount);
    System.out.println("Insert count is " + insertCount);
  }
  
  public void printStat(Cache[] db) {
    for (Map.Entry<String, Integer> entry : keyIDMap.entrySet()) {
      System.out.print(keyCountMap.get(entry.getKey()).getValue() + "\t");
      System.out.print(entry.getValue() + "\t");
      for (int i = 0; i < db.length; i++) {
        Value v = db[i].readCountMap.get(entry.getKey());
        if (v != null) {
          System.out.print(v.getValue() + "\t");
        } else {
          System.out.print(0 + "\t");
        }
      }
      System.out.println();
    }
  }
}


