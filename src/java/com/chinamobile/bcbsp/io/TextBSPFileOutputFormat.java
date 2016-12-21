/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.chinamobile.bcbsp.io;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.io.KeyValueBSPFileInputFormat.KVRecordReader;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPhdfsRW;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPhdfsTBFOF;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPhdfsRWImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPhdfsTBFOFImpl;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * TextBSPFileOutputFormat An example that extends
 * BSPFileOutputFormat<Text,Text>.
 */
public class TextBSPFileOutputFormat extends BSPFileOutputFormat<Text, Text> {

  /** This class can write a record in the format of Key-Value. */
  public static class LineRecordWriter extends RecordWriter<Text, Text> {
    /** Define LOG for outputting log information */
    private static final Log LOG = LogFactory.getLog(LineRecordWriter.class);
    /** Define encoding format */
    private static final String UTF8 = "UTF-8";
    /** Define an array, to store a row data */
    private static final byte[] NEW_LINE;
    static {
      try {
        NEW_LINE = "\n".getBytes(UTF8);
      } catch (UnsupportedEncodingException uee) {
        LOG.error("[NEW_LINE]", uee);
        throw new IllegalArgumentException("can't find " + UTF8 + " encoding");
      }
    }
    /** Define a data output stream */
    protected DataOutputStream out;
    /** Define a KV separator */
    private final byte[] keyValueSeparator;

    /**
     *
     * @param out A data output stream
     * @param keyValueSeparator a KV separator
     */
    public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
      this.out = out;
      try {
        this.keyValueSeparator = keyValueSeparator.getBytes(UTF8);
      } catch (UnsupportedEncodingException uee) {
        LOG.error("[LineRecordWriter]", uee);
        throw new IllegalArgumentException("can't find " + UTF8 + " encoding");
      }
    }

    /**
     *
     * @param out A data output stream
     */
    public LineRecordWriter(DataOutputStream out) {
      this(out, Constants.KV_SPLIT_FLAG);
    }

    /**
     * Write the object to the byte stream, handling Text as a special case.
     *
     * @param o
     *        the object to print
     * @throws IOException
     *         if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        out.write(o.toString().getBytes(UTF8));
      }
    }

    @Override
    public void write(Text key, Text value) throws IOException {
      boolean nullKey = key == null;
      boolean nullValue = value == null;
      if (nullKey && nullValue) {
        return;
      }
      if (!nullKey) {
        writeObject(key);
      }
      if (!(nullKey || nullValue)) {
        out.write(keyValueSeparator);
      }
      if (!nullValue) {
        writeObject(value);
      }
      out.write(NEW_LINE);
    }
    
    @Override
    public void write(Text keyValue) throws IOException {
      if (keyValue == null) {
        return;
      } else {
        writeObject(keyValue);
      }
      out.write(NEW_LINE);
    }

    @Override
    public void close(BSPJob job) throws IOException {
      out.close();
    }
  }

  @Override
  public RecordWriter<Text, Text> getRecordWriter(BSPJob job,
      StaffAttemptID staffId) throws IOException, InterruptedException {
    // Path file = getOutputPath(job, staffId);
    // FileSystem fs = file.getFileSystem(job.getConf());
    // FSDataOutputStream fileOut = fs.create(file, false);
    // return new LineRecordWriter(fileOut);
    // alter by gtt
    BSPhdfsTBFOF bspfileOut = new BSPhdfsTBFOFImpl(job, staffId);
    return new LineRecordWriter(bspfileOut.getFileOut());
  }

  @Override
  public RecordWriter<Text, Text> getRecordWriter(BSPJob job,
      StaffAttemptID staffId, Path writePath) throws IOException,
      InterruptedException {
    // Path file = getOutputPath(staffId, writePath);
    // FileSystem fs = file.getFileSystem(job.getConf());
    // FSDataOutputStream fileOut = fs.create(file, false);
    // return new LineRecordWriter(fileOut);
    // alter by gtt
    BSPhdfsRW bspFileOut = new BSPhdfsRWImpl(job, staffId, writePath);
    return new LineRecordWriter(bspFileOut.getFileOut());
  }
}
