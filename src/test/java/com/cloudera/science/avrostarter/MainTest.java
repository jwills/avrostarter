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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class MainTest {

  private File tmpFile;
  
  @Before
  public void setUp() throws Exception {
    tmpFile = File.createTempFile("foo", ".avro");
    tmpFile.deleteOnExit();
  }
  
  @Test
  public void testMain() throws Exception {
    Main m = new Main(FileSystem.getLocal(new Configuration()));
    m.run(new String[] { "10", tmpFile.getAbsolutePath() });
  }
}
