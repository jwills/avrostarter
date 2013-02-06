/**
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.science.avrostarter;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.science.avrostarter.avro.Call;
import com.google.common.collect.Lists;

/**
 *
 */
public class Main extends Configured implements Tool {
  
  private static final Log LOG = LogFactory.getLog(Main.class);
  private FileSystem fs;
  
  public Main() {
    this(null);
  }
  
  public Main(FileSystem fs) {
    this.fs = fs;
  }
  
  public Call makeCall(String center, String queue, String date, String fileName, byte[] data) {
    return Call.newBuilder()
        .setCallCenter(center)
        .setQueue(queue)
        .setDateString(date)
        .setFilename(fileName)
        .setData(ByteBuffer.wrap(data))
        .build();
  }
  
  public List<Call> makeCalls(int n) {
    List<Call> calls = Lists.newArrayList();
    for (int i = 0; i < n; i++) {
      // Fill in some random data
      calls.add(makeCall("Austin", "Q" + i, "2013-01-01", "foo" + (n - i), new byte[] { 2, 3 }));
    }
    return calls;
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      LOG.error("Usage: Main <num_calls_to_write> <output_file>");
      return 1;
    }
    if (fs == null) {
      fs = FileSystem.get(getConf());
    }
    List<Call> calls = makeCalls(Integer.parseInt(args[0]));
    String fileName = args[1];
    CallWriter writer = new CallWriter(fs);
    writer.write(calls, fileName);
    fs.close();
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new Main(), args);
  }
}
