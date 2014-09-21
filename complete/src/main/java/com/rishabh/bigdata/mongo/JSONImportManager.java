package com.rishabh.bigdata.mongo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rishabh.bigdata.log.Logger;

public class JSONImportManager {
	private static JSONImportManager _me;

	private JSONImportManager() {

	}

	public static JSONImportManager getInstance() {
		if (_me == null) {
			_me = new JSONImportManager();
		}
		return _me;
	}

	public boolean importDataFromFile(String mJSONFile, String mMongoCollection) {
		try {
			
			FileInputStream mJSONFileReaderStream = new FileInputStream(
					mJSONFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					mJSONFileReaderStream));

			String mJSONData = br.readLine();
			Logger.getInstance().logInfo("JSON File Import", "JSON Data read from " + mJSONFile);
			
			List<String> mFieldsList = new ArrayList<String>();
			List<String[]> mData = new ArrayList<String[]>();

			JSONObject mJSONObject = new JSONObject(mJSONData);

			JSONArray mFieldsListArray = mJSONObject.getJSONArray("fields");
			Logger.getInstance().logInfo("JSON File Import", "JSON Array Created");
			
			Integer mFieldListSize = mFieldsListArray.length();
			
			for (int loop = 0; loop < mFieldListSize; loop++) {
				JSONObject mFieldObject = mFieldsListArray.getJSONObject(loop);
				String mFieldName = mFieldObject.getString("label");
				String mUpdatedFieldName;
				if (mFieldName.contains(".")) {
					mUpdatedFieldName = mFieldName.replace('.', '-');
				} else {
					mUpdatedFieldName = mFieldName;
				}
				mFieldsList.add(mUpdatedFieldName);
			}
			
			Logger.getInstance().logInfo("JSON File Import", "Following fields loaded - " + mFieldsList.toString());
			
			JSONArray mDataListArray = mJSONObject.getJSONArray("data");
			Logger.getInstance().logInfo("JSON File Import", "JSON Array Created for data");
			
			Integer mDataListSize = mDataListArray.length();
			
			for (int loop = 0; loop < mDataListSize; loop++) {
				//String mDatax = mDataListArray.toString();
				//Logger.getInstance().logInfo("JSON File Import", "String of data loaded as " + mDatax);
				
				JSONArray mDataArray = mDataListArray.getJSONArray(loop);
				
				Integer mDataArraySize = mDataArray.length();
				
				String[] mDataValues = new String[mDataArraySize];
				
				for(int loopx = 0; loopx < mDataArraySize; loopx++) {
					mDataValues[loopx] = mDataArray.get(loopx).toString();
				}
				
				mData.add(mDataValues);
			}
			
			Logger.getInstance().logInfo("JSON File Import", "Data List created");
			
			MongoManager.getInstance("localhost", "SocialSciences").writeDataToCollection(mMongoCollection, mFieldsList, mData);
			
		} catch (FileNotFoundException e) {
			Logger.getInstance().logError("JSON File Import", e.getMessage());
		} catch (IOException e) {
			Logger.getInstance().logError("JSON File Import", e.getMessage());
		}

		return false;
	}
}
