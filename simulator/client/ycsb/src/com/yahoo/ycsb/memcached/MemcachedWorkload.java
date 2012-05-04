package com.yahoo.ycsb.memcached;

import java.util.Properties;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import java.util.HashMap;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The relative 
 * proportion of different kinds of operations, and other properties of the workload, are controlled
 * by parameters specified at runtime.
 * 
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldlength</b>: the size of each value (default: 100) This may not be a fixed length... Then just do random...
 * <LI><b>keylength</b>: the size of each key (default: 128)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate on - uniform, zipfian or latest (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed order ("hashed") (default: hashed)
 * </ul> 
 */
public class MemcachedWorkload
{
	/**
	 * Write lock to synchronize inserting to initkeymap
	 */
	private Object writeLock = new Object();
	
	private boolean isFirst = true;
	
	/**
	 * Store int to byte key mapping. For reuse bytes.
	 */
	private HashMap<Integer, String> IntKeyMap;

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
		recordcount=Integer.parseInt(p.getProperty(MemcachedClient.RECORD_COUNT_PROPERTY));
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
			
			int opcount=Integer.parseInt(p.getProperty(MemcachedClient.OPERATION_COUNT_PROPERTY));
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
		
		IntKeyMap = new HashMap<Integer, String>();
		
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
	public boolean doTransaction(MemcachedDB db, Object threadstate)
	{
		String op=operationchooser.nextString();
		
		if (isFirst) {
			op = "INSERT";
			isFirst = false;
		}
		
		if (op.compareTo("READ")==0)
		{
			doTransactionRead(db);
		}
		else if (op.compareTo("UPDATE")==0)
		{
			doTransactionUpdate(db);
		}
		else if (op.compareTo("INSERT")==0)
		{
			doTransactionInsert(db);
		}
		
		return true;
	}

	/**
	 * Read
	 * @param db Database
	 */
	public void doTransactionRead(MemcachedDB db)
	{
		int keynum;
		do {
			keynum=keychooser.nextInt();
			
		} while (keynum>transactioninsertkeysequence.lastInt());
		
		if (!orderedinserts)
		{
			keynum=Utils.hash(keynum);
		}
		
		String key = null;
		while (key == null) {
			key = IntKeyMap.get(keynum);
			if (key == null) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		long t1 = System.currentTimeMillis();
		db.read(key);
		long t2 = System.currentTimeMillis();
		int time = (int)(t2 - t1);
		
		int serverNum = db.getAccessedServerID();
		db.incrementKeyReadNum(keynum);
		db.incrementServerReadNum(serverNum);
		db.incrementServerLatency(serverNum, time);
	}
	
	/**
	 * Update
	 * @param db Database
	 */
	public void doTransactionUpdate(MemcachedDB db)
	{
		//choose a random key
		int keynum;
		do {
			keynum=keychooser.nextInt();
		} while (keynum>transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum=Utils.hash(keynum);
		}
		
		String key = null;
		while (key == null) {
			key = IntKeyMap.get(keynum);
		}
		String stringValue = Utils.ASCIIString(fieldlength);

		db.update(key, stringValue);
	}

	/**
	 * Insert
	 * @param db Database
	 */
	public void doTransactionInsert(MemcachedDB db)
	{
		//choose the next key
		int keynum=transactioninsertkeysequence.nextInt();
		if (!orderedinserts) {
			keynum=Utils.hash(keynum);
		}
		String stringKey = Utils.ASCIIString(keylength);
		String stringValue = Utils.ASCIIString(fieldlength);
		
		synchronized (writeLock) {
			IntKeyMap.put(keynum, stringKey);
		}
		
		long t1 = System.currentTimeMillis();
		db.insert(stringKey, stringValue);
		long t2 = System.currentTimeMillis();
		
		int time = (int)(t2 - t1);
		
		int serverNum = db.getAccessedServerID();
		db.incrementKeyReadNum(keynum);
		db.incrementServerReadNum(serverNum);
		db.incrementServerLatency(serverNum, time);
	}
}

