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

package com.chinamobile.bcbsp.ml;

import com.chinamobile.bcbsp.io.BSPFileInputFormat;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPHdfs;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPhdfsFSDIS;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPhdfsFSDISImpl;
import com.chinamobile.bcbsp.util.BSPJob;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * KeyValueBSPFileInputFormat An example that extends the
 * BSPFileInputFormat<Text, Text>. This class can support Key-Value pairs
 * InputFormat.
 */
public class KeyMatrixFileInputFormat extends BSPFileInputFormat<Text, Text> {

  /**
   * RecordReader
   *
   * This class can read data in the form of Key-Value.
   */
  public static class KVRecordReader extends RecordReader<Text, Text> {
    /** Define LOG for outputting log information */
    private static final Log LOG = LogFactory.getLog(KVRecordReader.class);
    /** Define a CompressionCodecFactory, used to compress
     * the configuration info*/
    private CompressionCodecFactory compressionCodecs = null;
    /** The split starting position */
    private long start;
    /** The split current location */
    private long pos;
    /** The split ending position */
    private long end;
    /** Define a line reader from an input stream. */
    private LineReader in;
    /** The maximum length of each line */
    private int maxLineLength;
    /** the vertex */
    private Text key = null;
    /** the whole list of edges of the vertex. */
    private Text value = null;
    /** Define a separator */
    private String separator = "\t";

    @Override
    public void initialize(InputSplit genericSplit, Configuration conf)
        throws IOException, InterruptedException {
      FileSplit split = (FileSplit) genericSplit;
      this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",
          Integer.MAX_VALUE);
      this.separator = conf.get("key.value.separator.in.input.line", "\t");
      start = split.getStart();
      end = start + split.getLength();
      // final Path file = split.getPath();
      BSPHdfs hdfsIni = new BSPHdfsImpl();
      compressionCodecs = new CompressionCodecFactory(conf);
      // final CompressionCodec codec = compressionCodecs.getCodec(file);
      final CompressionCodec codec = compressionCodecs.getCodec(hdfsIni
          .hdfsinitialize(split));
      // FileSystem fs = file.getFileSystem(conf);
      // FSDataInputStream fileIn = fs.open(split.getPath());
      BSPhdfsFSDIS bspfileIn = new BSPhdfsFSDISImpl(split, conf);
      boolean skipFirstLine = false;
      if (codec != null) {
        in = new LineReader(codec.createInputStream(bspfileIn.getFileIn()),
            conf);
        end = Long.MAX_VALUE;
      } else {
        if (start != 0) {
          skipFirstLine = true;
          --start;
          bspfileIn.seek(start);
        }
        in = new LineReader(bspfileIn.getFileIn(), conf);
      }
      if (skipFirstLine) {
        start += in.readLine(new Text(), 0,
            (int) Math.min((long) Integer.MAX_VALUE, end - start));
      }
      this.pos = start;
    }

    @Override
    public synchronized void close() throws IOException {
      if (in != null) {
        in.close();
      }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      if (start == end) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (pos - start) / (float) (end - start));
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      Text line = new Text();
      if (key == null) {
        key = new Text();
      }
      if (value == null) {
        value = new Text();
      }
      int newSize = 0;
      while (pos < end) {
        newSize = in.readLine(line, maxLineLength, Math.max(
            (int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
        if (null != line) {
          String[] kv = line.toString().split(this.separator);
          if (kv.length == 1) {
            key.set(kv[0]);
            value.set("0");
          } else {
            key.set(line.toString());
            value.set(kv[1]);

          }
        }
        if (newSize == 0) {
          break;
        }
        pos += newSize;
        if (newSize < maxLineLength) {
          break;
        }
        LOG.info("Skipped line of size " + newSize + " at pos " +
          (pos - newSize));
      }
      if (newSize == 0) {
        key = null;
        value = null;
        return false;
      } else {
        return true;
      }
    }
  }

  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit split,
      BSPJob job) throws IOException, InterruptedException {
    return new KVRecordReader();
  }

  @Override
  protected boolean isSplitable(BSPJob job, Path file) {
    CompressionCodec codec = new CompressionCodecFactory(job.getConf())
        .getCodec(file);
    return codec == null;
  }
}
