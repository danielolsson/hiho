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

package co.nubetech.hiho.mapred.input;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * Updated version of {@link co.nubetech.hiho.mapreduce.lib.input.FileStreamInputFormat}.
 *
 */
public class FileStreamRecordReader implements
		RecordReader<Text, FSDataInputStream> {
	private FileSplit split;
	private FSDataInputStream stream;
	private boolean isRead = false;
	private String fileName;
	private JobConf configuration;
	private Reporter reporter;

	final static Logger logger = Logger
			.getLogger(co.nubetech.hiho.mapreduce.lib.input.FileStreamRecordReader.class);

	public FileStreamRecordReader(InputSplit split, JobConf conf, Reporter reporter) throws IOException {
		this.reporter = reporter;
		initialize(split, conf);
	}

	public void initialize(InputSplit genericSplit, JobConf conf)
			throws IOException {
		this.split = (FileSplit) genericSplit;
		this.configuration = conf;

	}

	@Override
	public void close() throws IOException {
		if (stream != null) {
			IOUtils.closeStream(stream);
		}
	}

	@Override
	public Text createKey() {
		logger.debug("Creating key");
		return new Text();
	}

	@Override
	public FSDataInputStream createValue() {
		logger.debug("Creating value");
		FSDataInputStream stream = null;
		Path file = split.getPath();
		logger.debug("Path is " + file);
		fileName = file.getName();
		try {
			FileSystem fs = file.getFileSystem(configuration);
			stream = new FSDataInputStream(fs.open(file));
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.debug("Opened stream");
		return stream;
	}

	@Override
	public float getProgress() throws IOException {
		return 0f;
		// dummy
	}

	@Override
	public boolean next(Text key, FSDataInputStream stream) throws IOException {
		logger.debug("Inside next");
		if (isRead) {
			return false;
		} else {
			key.set(fileName);
			isRead = true;
			return true;
		}
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
}
