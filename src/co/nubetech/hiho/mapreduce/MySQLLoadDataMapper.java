/**
 * Copyright 2010 Nube Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package co.nubetech.hiho.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.ivy.util.StringUtils;
import org.apache.log4j.Logger;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.hiho.common.HIHOConf;

public class MySQLLoadDataMapper extends
		Mapper<Text, FSDataInputStream, NullWritable, NullWritable> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.MySQLLoadDataMapper.class);
	private Connection conn;

	static enum Counters {
		NUM_OF_INSERTED_ROWS
	}

	public void setConnection(Connection con) throws IOException{
		conn=con;
	}

	public Connection getConnection(){
		return conn;
	}


	@Override
	protected void setup(Mapper.Context context) throws IOException,
			InterruptedException {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();

			String connString = context.getConfiguration().get(
					DBConfiguration.URL_PROPERTY);
			String username = context.getConfiguration().get(
					DBConfiguration.USERNAME_PROPERTY);
			String password = context.getConfiguration().get(
					DBConfiguration.PASSWORD_PROPERTY);

			logger.debug("Connection values are " + connString + " " + username
					+ "/" + password);
			connect(connString, username, password);

		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}

	}

	protected void connect(String connString, String username, String password)
			throws SQLException {
		conn = DriverManager.getConnection(connString, username, password);
	}

	private static final String utf8 = "UTF-8";
	private static final byte[] newline;
	static {
		try {
			newline = "\n".getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
	}

	@Override
	public void map(Text key, FSDataInputStream val, Context context)
			throws IOException, InterruptedException {

		conn=getConnection();
		com.mysql.jdbc.Statement stmt = null;
		String query;
		String querySuffix = context.getConfiguration().get(
				HIHOConf.LOAD_QUERY_SUFFIX);
		boolean hasHeaderLine = context.getConfiguration().getBoolean(
				HIHOConf.LOAD_HAS_HEADER, false);
		boolean keyIsTableName = context.getConfiguration().getBoolean(
				HIHOConf.LOAD_KEY_IS_TABLENAME, false);

		String[] columnNames = null;
		if (hasHeaderLine) {
			BufferedReader headerReader = new BufferedReader(
					new InputStreamReader(val));
			String header = headerReader.readLine();
			columnNames = header.split(",");
			val.seek(header.getBytes(utf8).length + newline.length);
		}
		try {

			stmt = (com.mysql.jdbc.Statement) conn
					.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
			stmt.setLocalInfileInputStream(val);
			query = "load data local infile 'abc.txt' into table"
					+ (keyIsTableName ? " " + key.toString() : "");
			query += " " + querySuffix;
			if (hasHeaderLine)
				query += " (" + StringUtils.join(columnNames, ",") + ")";
			logger.debug("stmt: " + query);
			int rows = stmt.executeUpdate(query);
			logger.debug(rows + " rows updated");
			context.getCounter(Counters.NUM_OF_INSERTED_ROWS).increment(rows);

		} catch (Exception e) {
			e.printStackTrace();
			stmt = null;
			throw new IOException(e);
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException s) {
				s.printStackTrace();
			}
		}
	}

	@Override
	protected void cleanup(Mapper.Context context) throws IOException,
			InterruptedException {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException s) {
			s.printStackTrace();
		}
	}

}
