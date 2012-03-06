package com.yahoo.ycsb.shard;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
* Client to do operations
* Properties to set:
* shardb.server.host=""
* shardb.server.port=""
* shardb.controller.host=""
* shardb.controller.port=""
* @author Hao Li
*
*/
public class ShardDBClient extends DB {
	
    public void init() throws DBException {

    }

    
    public int delete(String table, String key) {
    	return 1;
    }

    public int insert(String table, String key, HashMap<String, String> values) {
    	return 1;

    }

    public int read(String table, String key, Set<String> fields) {
    		return 1;

    }


    public int update(String table, String key, HashMap<String, String> values) {
    	return 1;
    }

    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, String>> result) {
    	return 1;
	
    }


	public int read(String table, String key, Set<String> fields,
			HashMap<String, String> result) {
		return 0;
	}

}


