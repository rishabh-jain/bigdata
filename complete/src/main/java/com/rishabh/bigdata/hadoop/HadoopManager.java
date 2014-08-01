package com.rishabh.bigdata.hadoop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.apache.hive.jdbc.HiveConnection;

import com.rishabh.bigdata.log.Logger;

public class HadoopManager {
	private static HadoopManager _me;
	
	private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver"; 
	private static Connection mHiveCon;
	
	private HadoopManager() {
		try {
			Class.forName(DRIVER_NAME);
			
			mHiveCon = DriverManager.getConnection("jdbc:hive2://192.168.3.11:10000/default", "hive", "");
			Logger.getInstance().logInfo("Connection to hive server 192.168.3.11 established", mHiveCon.getSchema());
			
		} catch(ClassNotFoundException ce) {
			Logger.getInstance().logError("JDBC driver class not found", DRIVER_NAME);
		} catch (SQLException e) {
			Logger.getInstance().logError("SQLException for 192.168.1.2 hive server", e.getMessage());
		}
	}
	
	public static HadoopManager getInstance() {
		if (_me == null) {
			_me = new HadoopManager();
		}
		return _me;
	}
	
	public List<String> getSuggestions(String mToken) {
		return null;
	}
}
