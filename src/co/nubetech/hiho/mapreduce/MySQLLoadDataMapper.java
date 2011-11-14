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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.ivy.util.StringUtils;
import org.apache.log4j.Logger;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.hiho.common.HIHOConf;

public class MySQLLoadDataMapper extends MapReduceBase implements
		Mapper<Text, FSDataInputStream, NullWritable, NullWritable> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.MySQLLoadDataMapper.class);
	private Connection conn;
	protected String querySuffix;
	protected boolean hasHeaderLine;
	protected boolean keyIsTableName;

	public void setConnection(Connection con) throws IOException{
		conn=con;
	}

	public Connection getConnection(){
		return conn;
	}


	public void configure(JobConf job) {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();

			String connString = job.get(DBConfiguration.URL_PROPERTY);
			String username = job.get(DBConfiguration.USERNAME_PROPERTY);
			String password = job.get(DBConfiguration.PASSWORD_PROPERTY);
			querySuffix = job.get(HIHOConf.LOAD_QUERY_SUFFIX);
			hasHeaderLine = job.getBoolean(HIHOConf.LOAD_HAS_HEADER, false);
			keyIsTableName = job.getBoolean(HIHOConf.LOAD_KEY_IS_TABLENAME, false);

			logger.debug("Connection values are " + connString + " " + username
					+ "/" + password);
			connect(connString, username, password);

		} catch (Exception e) {
			e.printStackTrace();
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

	public void map(Text key, FSDataInputStream val,
			OutputCollector<NullWritable, NullWritable> collector,
			Reporter reporter) throws IOException {

		conn=getConnection();
		com.mysql.jdbc.Statement stmt = null;
		String query;

		String[] tableNames = null;
		if (hasHeaderLine) {
			BufferedReader headerReader = new BufferedReader(new InputStreamReader(val));
			String header = headerReader.readLine();
			tableNames = header.split(",");
			val.seek(header.getBytes(utf8).length + newline.length);
		}
		try {

			stmt = (com.mysql.jdbc.Statement) conn
					.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
			stmt.setLocalInfileInputStream(val);
			query = "load data local infile 'abc.txt' into table"
					+ (keyIsTableName ? " " + key.toString() : "");
			if (hasHeaderLine)
				query += " (" + StringUtils.join(tableNames, ",") + ")";
			query += " " + querySuffix;
			// query += "mrTest fields terminated by ','";
			logger.debug("stmt: " + query);
			int rows = stmt.executeUpdate(query);
			logger.debug(rows + " rows updated");

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

	public void close() throws IOException {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException s) {
			s.printStackTrace();
		}
	}

}
