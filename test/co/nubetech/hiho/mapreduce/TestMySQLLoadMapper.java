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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

import co.nubetech.apache.hadoop.DBConfiguration;
import co.nubetech.hiho.common.HIHOConf;

public class TestMySQLLoadMapper {

	private static final String QUERY_SUFFIX = "fields terminated by ','";

	@Test
	public final void testSetup() throws Exception {

		Context context = mock(Context.class);
		MySQLLoadDataMapper mapper = new MySQLLoadDataMapper() {
			protected void connect(String curl, String u, String p) {	}
		};
		Configuration conf = new Configuration();
		String url = "jdbc:mysql://localhost:3306/hiho";
		String usrname = "root";
		String password = "newpwd";
		conf.set(DBConfiguration.URL_PROPERTY, url);
		conf.set(DBConfiguration.USERNAME_PROPERTY, usrname);
		conf.set(DBConfiguration.PASSWORD_PROPERTY, password);
		when(context.getConfiguration()).thenReturn(conf);
		mapper.setup(context);
		verify(context, times(3)).getConfiguration();
	}

	class MyInputStream extends InputStream implements Seekable, PositionedReadable {
		boolean isRead = false;
		@Override
		public int read() throws IOException {assert(false); return 0;}
		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (!isRead) {
				byte[] b2 = "col1,col2,col3\n".getBytes("UTF-8");
				System.arraycopy(b2, 0, b, 0, b2.length);
				isRead = true;
				return b.length;
			} else {
				return -1;
			}
		}
		@Override
		public int read(long position, byte[] buffer, int offset, int length)
				throws IOException {assert(false); return 0;}
		@Override
		public void readFully(long position, byte[] buffer, int offset,
				int length) throws IOException {}
		@Override
		public void readFully(long position, byte[] buffer) throws IOException {}
		@Override
		public void seek(long pos) throws IOException {}
		@Override
		public long getPos() throws IOException {return 0;}
		@Override
		public boolean seekToNewSource(long targetPos) throws IOException {return false;}
	}

	@Test
	public final void testMapper() throws Exception {
		runMapper("tablename");
	}

	@Test
	public final void testMapperWithPartFilenames() throws Exception {
		runMapper("tablename-m-00005");
	}

	/**
	 * @param string
	 * @throws IOException
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	private void runMapper(String tablename) throws IOException, SQLException,
			InterruptedException {
		Context context = mock(Context.class);
		MySQLLoadDataMapper mapper = new MySQLLoadDataMapper();
		FSDataInputStream val;
		val = new FSDataInputStream(new MyInputStream());
		Connection con = mock(Connection.class);
		com.mysql.jdbc.Statement stmt = mock(com.mysql.jdbc.Statement.class);
		mapper.setConnection(con);
		String query = "load data local infile 'abc.txt' into table tablename " + QUERY_SUFFIX + " (col1,col2,col3)";
		when(
				 con.createStatement(
						ResultSet.TYPE_SCROLL_SENSITIVE,
						ResultSet.CONCUR_UPDATABLE)).thenReturn(stmt);
		Configuration conf = new Configuration();
		conf.set(HIHOConf.LOAD_QUERY_SUFFIX, QUERY_SUFFIX);
		conf.setBoolean(HIHOConf.LOAD_KEY_IS_TABLENAME, true);
		conf.setBoolean(HIHOConf.LOAD_HAS_HEADER, true);
		when(context.getConfiguration()).thenReturn(conf);
		when(stmt.executeUpdate(query)).thenReturn(10);
		Counter counter = mock(Counter.class);
		when(context.getCounter("MySQLLoadCounters","ROWS_INSERTED_TABLE_tablename")).thenReturn(counter);
		when(context.getCounter("MySQLLoadCounters","ROWS_INSERTED_TOTAL")).thenReturn(counter);
		mapper.map(new Text(tablename), val, context);
		verify(stmt).setLocalInfileInputStream(val);
		verify(stmt).executeUpdate(query);
		verify(counter, times(2)).increment(10);
	}
}
