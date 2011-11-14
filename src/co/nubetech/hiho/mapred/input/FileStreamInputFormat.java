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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Updated version of {@link co.nubetech.hiho.mapreduce.lib.input.FileStreamInputFormat}.
 *
 * This class returns file streams as records which can be used directly for
 * load data infile operations of databases The key is the filename
 *
 * @author sgoyal
 *
 */

public class FileStreamInputFormat extends
		FileInputFormat<Text, FSDataInputStream> {

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}

	@Override
	public RecordReader<Text, FSDataInputStream> getRecordReader(
			InputSplit split, JobConf conf, Reporter reporter) throws IOException {
		return new FileStreamRecordReader(split, conf, reporter);
	}

}
