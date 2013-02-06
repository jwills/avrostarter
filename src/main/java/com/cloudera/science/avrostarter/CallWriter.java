/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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

import java.io.OutputStream;
import java.util.List;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.science.avrostarter.avro.Call;

/**
 *
 */
public class CallWriter {

  private final FileSystem fs;
  private final DatumWriter<Call> datumWriter;
  
  public CallWriter(FileSystem fs) {
    this.fs = fs;
    this.datumWriter = new SpecificDatumWriter<Call>(Call.class);
  }
  
  public void write(List<Call> calls, String fileName) throws Exception {
    DataFileWriter<Call> dfw = new DataFileWriter<Call>(datumWriter);
    OutputStream os = fs.create(new Path(fileName), true);
    dfw.create(Call.SCHEMA$, os);
    for (Call call : calls) {
      dfw.append(call);
    }
    dfw.close();
  }
}
