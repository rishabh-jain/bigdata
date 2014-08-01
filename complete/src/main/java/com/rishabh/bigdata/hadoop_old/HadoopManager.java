package com.rishabh.bigdata.hadoop_old;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.ClosedChannelException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import com.google.protobuf.ByteString.Output;
import com.rishabh.bigdata.log.Logger;

public class HadoopManager {

	private static HadoopManager _me;

	private Configuration mConfig;
	private FileSystem mFileSystem;

	/*
	 * File system security has been disabled on this server
	 */

	private HadoopManager() throws IOException {
		mConfig = new Configuration();
		mConfig.set("fs.defaultFS", "hdfs://192.168.1.6:9001/user/rishabh");
		mConfig.set("hadoop.home.dir", "/Applications/hadoop/");
		mConfig.set("hadoop.user.name", "rishabh");
		mConfig.set("mapred.job.tracker", "192.168.1.6:8021");
		mConfig.set("mapreduce.framework.name", "yarn");
		mConfig.set("yarn.resourcemanager.address", "192.168.1.6:8032");
		mConfig.set("mapred.remote.os", "Linux");
		mConfig.set(
				"mapreduce.application.classpath",
				"/Applications/hadoop/etc/hadoop/*,/Applications/hadoop/share/hadoop/common/*,/Applications/hadoop/share/hadoop/common/lib/*,/Applications/hadoop/share/hadoop/hdfs/*,/Applications/hadoop/share/hadoop/hdfs/lib/*,/Applications/hadoop/share/hadoop/mapreduce/*,/Applications/hadoop/share/hadoop/mapreduce/lib/*,/Applications/hadoop/share/hadoop/yarn/*,/Applications/hadoop/share/hadoop/yarn/lib/*");

		mFileSystem = DistributedFileSystem.get(mConfig);
		Logger.getInstance().logInfo("Hadoop Manager",
				"File System with id " + mFileSystem.toString() + " loaded");
		Logger.getInstance().logInfo("Hadoop Manager",
				"File System URI " + mFileSystem.getUri().toString());

	}

	public static HadoopManager getInstance() {
		if (_me == null)
			try {
				_me = new HadoopManager();
				Logger.getInstance().logInfo("Hadoop Manager",
						"Hadoop Manager instantiated");
			} catch (IOException e) {
				Logger.getInstance().logError("Hadoop Manager", e.getMessage());
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		return _me;
	}

	/*
	 * Write a new file to HDFS .bd (bigdata) file extention is to denote
	 * bigdata files
	 */
	public boolean writeFileToHDFS(HadoopFile mHDFSFile, String mCollection, HDFSFileType mFileType) {
		try {
			Calendar cal = Calendar.getInstance();
			
			Path mHDFSFilePath;
			
			if (mFileType.equals(HDFSFileType.MongoBackup)) {
				 mHDFSFilePath = new Path(mFileSystem.getUri().toString()
						+ "/user/rishabh/User/Rishabh/mongoBackup/" + mCollection + "/" + mHDFSFile.mTitle + "_"+ cal.getTimeInMillis() + ".bd");
			} else {
				mHDFSFilePath = new Path(mFileSystem.getUri().toString()
						+ "/user/rishabh/User/Rishabh/" + mHDFSFile.mTitle + "_"+ cal.getTimeInMillis() + ".bd");
			}
			

			if (mFileSystem.exists(mHDFSFilePath)) {
				mFileSystem.delete(mHDFSFilePath, true);
			}
			
			Logger.getInstance().logInfo("Hadoop New File Writer",
					"Started to write data to " + mHDFSFilePath.toString());

			final FSDataOutputStream mOutputStream = mFileSystem
					.create(mHDFSFilePath);

			BufferedWriter mBufferedWriter = new BufferedWriter(
					new OutputStreamWriter(mOutputStream, "UTF-8"));

			String fileData = mHDFSFile.mData;
				if (fileData != null)
					mBufferedWriter.write(fileData);
			
			Logger.getInstance().logInfo("Hadoop New File Writer",
					"Data written to " + mHDFSFilePath.toString() + " with size " + mOutputStream.size() / (1024 * 1024) + " MBs");

			mOutputStream.close();
			mBufferedWriter.close();
			mFileSystem.close();

			return true;

		} catch(ClosedChannelException channelException) {
			Logger.getInstance().logError("Hadoop New File Writer", channelException.getMessage());
			return true;
		}
		catch (IOException e) {
			Logger.getInstance().logError("Hadoop New File Writer", e.getMessage());
		}

		return false;
	}

	/*public boolean runMapReduce(HadoopFile mHDFSFile) {
		try {
			Path mHDFSFilePath = new Path(mFileSystem.getUri().toString()
					+ "/user/rishabh/" + mHDFSFile.mTitle + ".bd");

			Path mHDFSOutputFilePath = new Path(mFileSystem.getUri().toString()
					+ "/user/rishabh/" + mHDFSFile.mTitle + ".out");

			Job mJob = Job.getInstance(mConfig);
			mJob.setJarByClass(this.getClass());
			mJob.setJobName("Hadoop File");

			FileInputFormat.addInputPath(mJob, mHDFSFilePath);
			FileOutputFormat.setOutputPath(mJob, mHDFSOutputFilePath);

			mJob.setMapperClass(HadoopMapper.class);
			mJob.setReducerClass(HadoopReducer.class);

			mJob.setOutputKeyClass(Text.class);
			mJob.setOutputValueClass(IntWritable.class);

			Logger.getInstance().logInfo("Hadoop Manager - Map Reduce",
					"Sending Job for map-reduction");
			return (mJob.waitForCompletion(true) ? true : false);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			Logger.getInstance().logError("Hadoop Manager - Map Reduce",
					e.getMessage());
		}

		return false;
	}

	public boolean findFileInHDFS(String mFileTitle, String mCollection) {
		try {
			Logger.getInstance().logInfo("Find HDFS File",
					"Trying to find file in HDFS");
			Path mHDFSFilePath = new Path(mFileSystem.getUri().toString()
					+ "/user/rishabh/User/Rishabh/" + mCollection + "/" + mFileTitle + ".bd");

			if (mFileSystem.exists(mHDFSFilePath)) {
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
			Logger.getInstance().logError("Find HDFS File", e.getMessage());
		}

		return false;
	}

	/*public boolean writeToExisitingFileInHDFS(HadoopFile mHDFSFile, String mCollection) {
		try {
			Path mHDFSFilePath = new Path(mFileSystem.getUri().toString()
					+ "/user/rishabh/User/Rishabh/" + mCollection + "/" + mHDFSFile.mTitle + ".bd");

			
			if (mFileSystem.exists(mHDFSFilePath)) {
				final FSDataOutputStream mOutputStream = mFileSystem
						.append(mHDFSFilePath);

				Logger.getInstance().logInfo("Write To Existing HDFS File",
						"Started to write data to " + mHDFSFilePath.toString());
				
				BufferedWriter mBufferedWriter = new BufferedWriter(
						new OutputStreamWriter(mOutputStream, "UTF-8"));

				String fileData = mHDFSFile.mData;
					if (fileData != null)
						mBufferedWriter.append(fileData);
				
				Logger.getInstance().logInfo("Write To Existing HDFS File",
						"Data written to " + mHDFSFilePath.toString() + " with file size of " + mOutputStream.size() + " bytes");

				mOutputStream.close();
				mBufferedWriter.close();
				mFileSystem.close();
				
				return true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			Logger.getInstance().logError("Write To Existing HDFS File",
					e.getMessage());
		}

		return false;
	}*/
}
