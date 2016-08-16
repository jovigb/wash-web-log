package com.weblog;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsUtils {

	protected HdfsUtils() {}

	public static void deleteDir(Configuration conf, Path path) throws IOException {
		FileSystem fs = FileSystem.get(conf);

		fs.deleteOnExit(path);
		fs.close();
	}

	public static RemoteIterator<LocatedFileStatus> listDir(Configuration conf, String dir) throws IOException {
		FileSystem fs = FileSystem.get(conf);

		RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path(dir), false);
		fs.close();
		return ri;
	}

	public static ResultSet execHive(String sql) throws SQLException {
		String driverName = "org.apache.hive.jdbc.HiveDriver";
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		String HOST = "cdh179";
		String PORT = "10000";
		ResultSet res = null;
		
		Connection con = DriverManager.getConnection("jdbc:hive2://" + HOST + ":" + PORT + "/;auth=noSasl", "hive",
				"hive");
		Statement stmt = con.createStatement();
		System.out.println("Running: " + sql);
		if (sql.toLowerCase().startsWith("select"))
			res = stmt.executeQuery(sql);
		else
			stmt.execute(sql);
//		while (res.next()) {
//			System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + String.valueOf(res.getLong(3)) + "\t"
//					+ res.getString(4));
//		}
		stmt.close();
		con.close();
		return res;
	}

	public static ResultSet execImpala(String sql) throws SQLException {
		String driverName = "org.apache.hive.jdbc.HiveDriver";
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		String HOST = "cdh179";
		String PORT = "21050";
		ResultSet res = null;
		
		Connection con = DriverManager.getConnection("jdbc:hive2://" + HOST + ":" + PORT + "/;auth=noSasl", "hive",
				"hive");
		Statement stmt = con.createStatement();
		System.out.println("Running: " + sql);
		if (sql.toLowerCase().startsWith("select"))
			res = stmt.executeQuery(sql);
		else
			stmt.execute(sql);
//		while (res.next()) {
//			System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + String.valueOf(res.getLong(3)) + "\t"
//					+ res.getString(4));
//		}
		stmt.close();
		con.close();
		return res;
	}
	
}
