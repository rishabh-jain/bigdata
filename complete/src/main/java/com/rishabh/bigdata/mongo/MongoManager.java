package com.rishabh.bigdata.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Set;
import java.util.StringTokenizer;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.rishabh.bigdata.hadoop.HDFSFileType;
import com.rishabh.bigdata.hadoop.HadoopFile;
import com.rishabh.bigdata.hadoop.HadoopManager;
import com.rishabh.bigdata.log.Logger;

public class MongoManager {

	private static MongoManager _me;

	private MongoClient mMongoClient;
	private DB mAnalysisDatabase;
	private DBCollection mCollection;
	private DBCursor mDBCursor;
	private DBObject mDBObject;
	private BasicDBObject mDBQuery;
	private BasicDBObject mFieldsProject;

	private HadoopFile mHDFSFile;

	private MongoManager(String mServerAddress, String mAnalysisDB)
			throws UnknownHostException {
		mMongoClient = new MongoClient(mServerAddress);
		mAnalysisDatabase = mMongoClient.getDB(mAnalysisDB);
	}

	public static MongoManager getInstance(String mServerAddress,
			String mAnalysisDB) {
		if (_me == null)
			try {
				_me = new MongoManager(mServerAddress, mAnalysisDB);
			} catch (UnknownHostException e) {
				Logger.getInstance().logError("Mongo Manager", e.getMessage());
			}
		return _me;
	}

	public HadoopFile getCollectionKeys(String mCollectionName) {
		mCollection = mAnalysisDatabase.getCollection(mCollectionName);

		int pageSize = 500;
		
		mDBCursor = mCollection.find();
		mDBCursor.batchSize(pageSize);

		Set<String> mKeySet;

		StringBuilder mKeySetString = new StringBuilder();

		StringTokenizer mStringTokenizer = null;

		int flag = 0;

		while (mDBCursor.hasNext()) {
			mKeySet = mDBCursor.next().keySet();
			mStringTokenizer = new StringTokenizer(mKeySet.toString(), "[]");

			if (flag == 0) {
				mKeySetString.append(mStringTokenizer.nextToken());
				flag++;
			} else {
				mKeySetString.append(System.getProperty("line.separator")
						+ mStringTokenizer.nextToken());
			}
		}

		Logger.getInstance().logInfo("Mongo Manager",
				"Key set for " + mCollectionName + " collection loaded");

		mHDFSFile = new HadoopFile();

		mHDFSFile.mTitle = "KeySet_" + mCollectionName;
		mHDFSFile.mData = mKeySetString.toString();

		return mHDFSFile;
	}

	/*
	 * Backs up the Data from mongo collection to the HDFS with paging of 2500 documents
	 */
	public String backupData(String mBackupCollection) {
		try {
			
			int pageSize = 2500;
			
			Boolean mIsHDFSFileWritten;
			
			mCollection = mAnalysisDatabase.getCollection("MetaData");

			mDBQuery = new BasicDBObject();

			mDBQuery.put("title", mBackupCollection + "Backup");

			mDBCursor = mCollection.find(mDBQuery);

			
			if (mDBCursor.hasNext()) {
				mDBObject = mDBCursor.next();
			} else {
				return null;
			}
			
			String mBackupLimit = mDBObject.get("backupLimit").toString();

			mCollection = mAnalysisDatabase.getCollection(mBackupCollection);

			mDBCursor = mCollection.find();

			Logger.getInstance().logInfo("Mongo Data Backup",
					"Cursor for data from mongo loaded successfully");

			StringBuilder mFileData = new StringBuilder();

			Logger.getInstance().logInfo("Mongo Data Backup",
					"Creating hadoop file for backup");

			mDBCursor.batchSize(pageSize);	// for paging
			
			int mPageSizeCounter = 0;
			
			while (mDBCursor.hasNext()) {
				DBObject mDataObject = mDBCursor.next();
				
				mPageSizeCounter++;
				
				if (mPageSizeCounter > pageSize) {	// if page items are loaded, write it to HDFS and flush buffer
					mHDFSFile = new HadoopFile();
					mHDFSFile.mTitle = "_back" + mBackupCollection;
					mHDFSFile.mData = mFileData.toString();
					
					Logger.getInstance().logInfo("Mongo Data Backup",
							"New Hadoop File created with " + mPageSizeCounter + " records");
					
					mIsHDFSFileWritten = HadoopManager.getInstance().writeFileToHDFS(mHDFSFile, mBackupCollection, HDFSFileType.MongoBackup);
					
					if (!mIsHDFSFileWritten) {
						return "Error";
					}
					
					mFileData = new StringBuilder();
					mFileData.append(mDataObject.toString()
							+ System.getProperty("line.separator"));
					
					mPageSizeCounter = 0;
				} else {
				mFileData.append(mDataObject.toString()
						+ System.getProperty("line.separator"));
				}
			}

			mHDFSFile = new HadoopFile();

			mHDFSFile.mTitle = "_back" + mBackupCollection;
			mHDFSFile.mData = mFileData.toString();

			Logger.getInstance().logInfo("Mongo Data Backup",
					"New Hadoop File created with " + mPageSizeCounter + " records");

			mIsHDFSFileWritten = HadoopManager.getInstance().writeFileToHDFS(mHDFSFile, mBackupCollection, HDFSFileType.MongoBackup);
			
			if (!mIsHDFSFileWritten) {
				return "Error";
			}

			return "Success";
		} catch (Exception e) {
			Logger.getInstance().logError("Mongo Data Backup", e.getMessage());
		}
		return "Error";
	}
}
