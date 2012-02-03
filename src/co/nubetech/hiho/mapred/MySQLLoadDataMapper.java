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
package co.nubetech.hiho.mapred;

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

public class MySQLLoadDataMapper extends MapReduceBase
		implements Mapper<Text, FSDataInputStream, NullWritable, NullWritable> {

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.MySQLLoadDataMapper.class);
	private Connection conn;
	private boolean disableKeys;

	public void setConnection(Connection con) throws IOException{
		conn=con;
	}

	public Connection getConnection(){
		return conn;
	}


	@Override
	public void configure(JobConf job) {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();

			String connString = job.get(DBConfiguration.URL_PROPERTY);
			String username = job.get(DBConfiguration.USERNAME_PROPERTY);
			String password = job.get(DBConfiguration.PASSWORD_PROPERTY);

			logger.debug("Connection values are " + connString + " " + username
					+ "/" + password);
			connect(connString, username, password);

		} catch (Exception e) {
			e.printStackTrace();
		}
		querySuffix = job.get(HIHOConf.LOAD_QUERY_SUFFIX);
		hasHeaderLine = job.getBoolean(HIHOConf.LOAD_HAS_HEADER, false);
		keyIsTableName = job.getBoolean(HIHOConf.LOAD_KEY_IS_TABLENAME, false);
		disableKeys = job.getBoolean(HIHOConf.LOAD_DISABLE_KEYS, false);
	}

	protected void connect(String connString, String username, String password)
			throws SQLException {
		conn = DriverManager.getConnection(connString, username, password);
	}

	private static final String utf8 = "UTF-8";
	private static final byte[] newline;
	private String querySuffix;
	private boolean hasHeaderLine;
	private boolean keyIsTableName;
	static {
		try {
			newline = "\n".getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
	}

	/**
	 * Map file tables to DB tables. File name can be &lt;tablename&gt; or
	 * &lt;tablename&gt;-m-XXXXX.
	 *
	 * @param key the filename of the data stream
	 * @return the DB table name
	 * @see {@link
	 *      org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getUniqueFile}
	 */
	private String keyToTablename(Text key) {
		String filename = key.toString();
		if (filename.matches("\\w+-[mr]-[0-9]{5}"))
			return filename.substring(0, filename.length() - 8);
		else
			return filename;
	}

	@Override
	public void map(Text key, FSDataInputStream val,
			OutputCollector<NullWritable, NullWritable> collector,
			Reporter reporter) throws IOException {

		conn=getConnection();
		com.mysql.jdbc.Statement stmt = null;
		String query;

		String[] columnNames = null;
		if (hasHeaderLine) {
			BufferedReader headerReader = new BufferedReader(
					new InputStreamReader(val));
			String header = headerReader.readLine();
			if (header == null)
				return;
			columnNames = header.split(",");
			val.seek(header.getBytes(utf8).length + newline.length);
		}
		try {

			stmt = (com.mysql.jdbc.Statement) conn
					.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
			String tablename = (keyIsTableName ? keyToTablename(key) : "");
			if (disableKeys && !tablename.equals("")) {
				reporter.setStatus("Disabling keys on " + tablename);
				stmt.execute("ALTER TABLE " + tablename + " DISABLE KEYS");
			}
			stmt.setLocalInfileInputStream(val);
			query = "load data local infile 'abc.txt' into table "
					+ tablename + " ";
			query += querySuffix;
			if (hasHeaderLine)
				query += " (" + StringUtils.join(columnNames, ",") + ")";
			reporter.setStatus("Inserting into " + tablename);
			logger.debug("stmt: " + query);
			int rows = stmt.executeUpdate(query);
			logger.debug(rows + " rows updated");
			if (disableKeys && !tablename.equals("")) {
				reporter.setStatus("Re-enabling keys on " + tablename);
				stmt.execute("ALTER TABLE " + tablename + " ENABLE KEYS");
			}
			if (!tablename.equals(""))
				reporter.getCounter("MySQLLoadCounters","ROWS_INSERTED_TABLE_" + tablename).increment(rows);
			reporter.getCounter("MySQLLoadCounters","ROWS_INSERTED_TOTAL").increment(rows);

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
	public void close() throws IOException {
		try {
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException s) {
			s.printStackTrace();
			throw new IOException(s);
		}
	}

}
