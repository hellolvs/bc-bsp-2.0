/*
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

package com.chinamobile.bcbsp.pipes;

//import java.io.*;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;

import com.chinamobile.bcbsp.comm.IMessage;

/**
 * This protocol is a binary implementation of the Pipes protocol.
 */
public class BinaryProtocol implements DownwardProtocol {
  /**The current protocol version.*/
  public static final int CURRENT_PROTOCOL_VERSION = 0;
  /**
   * The buffer size for the command socket.
   */
  private static final int BUFFER_SIZE = 128 * 1024;
  /**A data output stream lets an application write primitive
   *Java data types to an output stream in a portable way.*/
  private DataOutputStream outPutstream;
  /**The data output buffer.*/
  private DataOutputBuffer buffer = new DataOutputBuffer();
  /**The log from log4j for user to write log.*/
  private static final Log LOG = LogFactory.getLog(BinaryProtocol.class
      .getName());
  /**A thread to read command from c++.*/
  private UplinkReaderThread uplink;
  /**The partition ID.*/
  private int partitionId = 0;
  /**The variable to keep the send message function synchronous.*/
  private Object mutex;

  /**
   * The integer codes to represent the different messages. These must match the
   * C++ codes or massive confusion will result.
   */
  private static enum MessageType {
    /**Command to start the c++ process.*/
    COMMAND_START(0),
    /**A flag to notify that the data is graph data.*/
    FLAG_GRAPHDATA(1),
    /**A flag to notify that the data is message.*/
    FLAG_MESSAGE(2),
    /**Command to request message.*/
    COMMAND_MESSAGE(3),
    /**Command to request aggregate value.*/
    COMMAND_AGGREGATE_VALUE(4),
    /**A flag to notify that the data is aggregate value.*/
    FLAG_AGGREGATE_VALUE(5),
    /**Command to run a superstep.*/
    COMMAND_RUN_SUPERSTEP(6),
    /**Command to start partition.*/
    COMMAND_PARTITION(7),
    /**A flag to notify that the data is partition.*/
    FLAG_PARTITION(8),
    /**Command to save result.*/
    COMMAND_SAVE_RESULT(9),
    /**A flag to notify that the data is staff id.*/
    STAFFID(50),
    /**Command to abort the compute task.*/
    COMMAND_ABORT(51),
    /**Command to end the job.*/
    COMMAND_DONE(52),
    /**Command to close the connect between java and c++.*/
    COMMAND_CLOSE(53);
    /**A variable to save the command or message type.*/
    private final int code;
    /**Set the code value.*/
    MessageType(int code) {
      this.code = code;
    }
  }
  /**A thread to read the command from c++.*/
  private class UplinkReaderThread extends Thread {
    /**A data input stream lets an application read primitive
     *Java data types from an underlying input stream in a
     *machine-independent way.*/
    private DataInputStream inStream;
    /**The upward protocol to send data to c++.*/
    private UpwardProtocol handler;
    /**The constructor.
     * @throws IOException if has error*/
    public UplinkReaderThread(InputStream stream, UpwardProtocol handler)
        throws IOException {
      inStream = new DataInputStream(new BufferedInputStream(stream,
          BUFFER_SIZE));
      this.handler = handler;
    }
    /**Close connection with c++.
     * @throws IOException if has error
     */
    public void closeConnection() throws IOException {
      inStream.close();
    }
    /**According the command type to call functions.*/
    public void run() {
      // test
     /* String rpath = "/home/bcbsp/inrun.txt";
      File rfilename = new File(rpath);
      RandomAccessFile rmm = null;
      try {
        rfilename.createNewFile();
        rmm = new RandomAccessFile(rfilename, "rw");
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }*/

      while (true) {
        try {
          // LOG.info("in while");
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
          }
          if (inStream.available() == 0) {
            Thread.sleep(20);
            continue;
          }
          int cmd = WritableUtils.readVInt(inStream);
          //LOG.info("Handling uplink command " + cmd);
          // test
         // rmm.writeBytes("command type = " + cmd + "\n");

          if (cmd == MessageType.COMMAND_MESSAGE.code) {

            String vertexId = Text.readString(inStream);
            LOG.info("vertexId is " + vertexId);
            ConcurrentLinkedQueue<IMessage> messages = handler
                .getMessage(vertexId);

            // 由于消息的条数不是固定的，需要有机制来保证对面c++端知道什么时候，消息接收完毕， 这块采用计数
            // 先发送消息的条数，这样对面收到后，开始计数，计数到消息的条数认为停止
            LOG.info("messages size is " + messages.size());
            // 先发命令
            WritableUtils
                .writeVInt(outPutstream, MessageType.FLAG_MESSAGE.code);

            WritableUtils.writeVInt(outPutstream, messages.size());
            if (0 != messages.size()) {
              Iterator<IMessage> it = messages.iterator();
              while (it.hasNext()) {
                String temp = "";
                try {
                  temp = it.next().intoString();
                  // LOG.info("in command_message the message is "
                  // + temp);
                  BinaryProtocol.this.sendMessage(temp);
                } catch (Exception e) {
                  LOG.error("<collectMessages>", e);
                }
              }
            }
            BinaryProtocol.this.flush();
            LOG.info("New messages send over");
          } else if (cmd == MessageType.FLAG_MESSAGE.code) {
            int size = WritableUtils.readVInt(inStream);
            int i = 0;
           // LOG.info("message size is " + size);
            while (i < size) {
              String temp = null;
              temp = Text.readString(inStream);
              // LOG.info("message " + i + " is " + temp);
              handler.sendNewMessage(temp);
              i++;
            }
          } else if (cmd == MessageType.FLAG_AGGREGATE_VALUE.code) {
            // c++ 端发送格式：命令 + 发送的聚集值的个数 + 聚集值
            int size = WritableUtils.readVInt(inStream);
            //LOG.info("aggregate vlaue size is " + size);
            int i = 0;
            while (i < size) {
              String s = Text.readString(inStream);
             // LOG.info("aggregate value is " + s + "  i=" + i);
             // LOG.info("before set aggregatevalue");
              handler.setAgregateValue(s);
             // LOG.info("after set aggregatevalue");
              i++;
            }
            LOG.info("after receive aggregatevalue");
          } else if (cmd == MessageType.FLAG_PARTITION.code) {
            BinaryProtocol.this.partitionId = WritableUtils.readVInt(inStream);
            synchronized (mutex) {
              mutex.notify();
            }
          } else if (cmd == MessageType.COMMAND_DONE.code) {

            LOG.info("Pipe current superStep done");
            handler.currentSuperStepDone();
          } else if (cmd == MessageType.FLAG_GRAPHDATA.code) {
            // c++ 端发送的数据格式为：命令 + 图顶点的个数 　+　（vertexid + vertexValue
            // + 图顶点对应的边的个数 + edges）。。。
            //
            // 指定文件路径和名称just for test-->
            //String path = "/home/bcbsp/suncity.txt";
            //File filename = new File(path);
           // filename.createNewFile();
            //RandomAccessFile mm = null;
            int vertexSize = WritableUtils.readVInt(inStream);
            //BufferedWriter output1 = new BufferedWriter(new FileWriter(
                //"/home/bcbsp/test.txt"));
            String save;
            LOG.info("+++++++++++++++++++++" + vertexSize + "++++++++");
            save = cmd + "save result" + vertexSize;

            //mm = new RandomAccessFile(filename, "rw");
            //mm.writeBytes(save);

            handler.openDFS();
            int i = 0;
            String vertexEdge;
            while (i < vertexSize) {
              vertexEdge = Text.readString(inStream);
              handler.saveResult(vertexEdge);
              i++;
            }
            LOG.info("++++++++++++before start hander saveResult++++++++");
            handler.closeDFS();
          } else {
            throw new IOException("Bad command code: " + cmd);
          }
        } catch (InterruptedException e) {
          LOG.info(""); // TODO notic
          return;
        } catch (Throwable e) {
          LOG.info("error" + StringUtils.stringifyException(e));
          handler.failed(e);
          return;
        }
      }
    }
  }

  /**
   * An output stream that will save a copy of the data into a file.
   */
  private static class TeeOutputStream extends FilterOutputStream {
    /** An output stream that will save a copy of the data into a file.*/
    private OutputStream file;
    /**The constructor.
     * @param filename
     *         the file name that is writing to
     * @param base
     *        the outputStream used to write to the file
     * @throws IOException*/
    TeeOutputStream(String filename, OutputStream base) throws IOException {
      super(base);
      file = new FileOutputStream(filename);
    }
    /**Writes len bytes from the specified byte array starting at offset
     * off to this output stream.
     * @param  b the data.
     * @param  off the start offset in the data.
     * @param  len the number of bytes to write.

     * @throws IOException */
    public void write(byte b[], int off, int len) throws IOException {
      file.write(b, off, len);
      out.write(b, off, len);
    }
    /**Writes the specified byte to this output stream.
     * @param  b the data.
     * @throws IOException */
    public void write(int b) throws IOException {
      file.write(b);
      out.write(b);
    }
    /**Flushes this output stream and forces any buffered output
     * bytes to be written out to the stream.
     * @throws IOException */
    public void flush() throws IOException {
      file.flush();
      out.flush();
    }
    /**Closes this output stream and releases any system resources
     * associated with the stream.
     * @throws IOException */
    public void close() throws IOException {
      flush();
      file.close();
      out.close();
    }
  }

  /**
   * Create a proxy object that will speak the binary protocol on a socket.
   * Upward messages are passed on the specified handler and downward downward
   * messages are public methods on this object.
   * @param sock
   *        The socket to communicate on.
   * @param handler
   *        The handler for the received messages.
   *
   * @throws IOException e
   */

  public BinaryProtocol(Socket sock, UpwardProtocol handler)
      throws IOException {
    OutputStream raw = sock.getOutputStream();
    // If we are debugging, save a copy of the downlink commands to a file
    // just for test
    // raw = new TeeOutputStream("downlink.txt", raw);
    outPutstream = new DataOutputStream(new BufferedOutputStream(raw,
        BUFFER_SIZE));
    uplink = new UplinkReaderThread(sock.getInputStream(), handler);
    uplink.setName("pipe-uplink-handler");
    uplink.start();
  }

  /**
   * Close the connection and shutdown the handler thread.
   * @throws IOException e
   *
   * @throws InterruptedException
   */
  public void close() throws IOException, InterruptedException {
    LOG.debug("closing connection");
    outPutstream.close();
    uplink.closeConnection();
    uplink.interrupt();
    uplink.join();
  }
  /**Send the start command to c++ process and start it.
   * @throws IOException e*/
  public void start() throws IOException {
    LOG.debug("starting downlink");
    WritableUtils.writeVInt(outPutstream, MessageType.COMMAND_START.code);
    WritableUtils.writeVInt(outPutstream, CURRENT_PROTOCOL_VERSION);
    flush();
  }
  /**Send the close command to c++ process .
   * @throws IOException e*/
  public void endOfInput() throws IOException {
    WritableUtils.writeVInt(outPutstream, MessageType.COMMAND_CLOSE.code);
    this.flush();
    LOG.info("debug:Sent close command");
  }
  /**Send the abort command to c++ process.
   * @throws IOException e*/
  public void abort() throws IOException {
    WritableUtils.writeVInt(outPutstream, MessageType.COMMAND_ABORT.code);
    this.flush();
    LOG.debug("Sent abort command");
  }
  /**Flushes this output stream and forces any buffered output
   * bytes to be written out to the stream.
   * @throws IOException e
   */
  public void flush() throws IOException {
    outPutstream.flush();
  }

  // /**
  // * Write the given object to the stream. If it is a Text or BytesWritable,
  // * write it directly. Otherwise, write it to a buffer and then write the
  // * length and data to the stream.
  // * @param obj the object to write
  // * @throws IOException
  // */
  // private void writeObject(Writable obj) throws IOException {
  // // For Text and BytesWritable, encode them directly, so that they end up
  // // in C++ as the natural translations.
  // if (obj instanceof Text) {
  // Text t = (Text) obj;
  // int len = t.getLength();
  // WritableUtils.writeVInt(outPutstream, len);
  // outPutstream.write(t.getBytes(), 0, len);
  // } else if (obj instanceof BytesWritable) {
  // BytesWritable b = (BytesWritable) obj;
  // int len = b.getLength();
  // WritableUtils.writeVInt(outPutstream, len);
  // outPutstream.write(b.getBytes(), 0, len);
  // } else {
  // buffer.reset();
  // obj.write(buffer);
  // int length = buffer.getLength();
  // WritableUtils.writeVInt(outPutstream, length);
  // outPutstream.write(buffer.getData(), 0, length);
  // }
  // }

  @Override
  public void runASuperStep() throws IOException {
    WritableUtils.writeVInt(outPutstream,
        MessageType.COMMAND_RUN_SUPERSTEP.code);
    this.flush();
    LOG.info("Run a superStep  command");
  }

  @Override
  public void sendMessage(String message) throws IOException {
    // LOG.info("the message is " + message);
    Text.writeString(outPutstream, message);
    // LOG.info("Run send messages  command");

  }

  @Override
  public void sendStaffId(String staffId) throws IOException {
    WritableUtils.writeVInt(outPutstream, MessageType.STAFFID.code);
    LOG.info("the staffid is : " + staffId);
    Text.writeString(outPutstream, staffId);
    LOG.info("Run send staffid command");
    this.flush();
  }

  @Override
  public void sendKeyValue(String key, String value) throws IOException {
    WritableUtils.writeVInt(outPutstream, MessageType.FLAG_GRAPHDATA.code);
    Text.writeString(outPutstream, key);
    Text.writeString(outPutstream, value);
    this.flush();
    // LOG.info("Run a send graph data command key/value");
  }

  @Override
  // num = staff num
  public void sendKey(String key, int num) throws IOException,
      InterruptedException {
    WritableUtils.writeVInt(outPutstream, MessageType.COMMAND_PARTITION.code);
    Text.writeString(outPutstream, key);
    WritableUtils.writeVInt(outPutstream, num);
    synchronized (mutex) {
      mutex.wait();
    }
  }

  @Override
  public int getPartionId() throws IOException {
    // TODO Auto-generated method stub
    return partitionId;
  }

  @Override
  public void sendNewAggregateValue(String[] aggregateValue)
      throws IOException {
    LOG.info("send new aggregateValue!");
    WritableUtils
        .writeVInt(outPutstream, MessageType.FLAG_AGGREGATE_VALUE.code);
    for (int i = 0; i < aggregateValue.length; i++) {
      WritableUtils.writeVInt(outPutstream, aggregateValue.length);
      Text.writeString(outPutstream, aggregateValue[i]);
    }
  }

  @Override
  public void saveResult() throws IOException {
    // TODO Auto-generated method stub
    WritableUtils.writeVInt(outPutstream, MessageType.COMMAND_SAVE_RESULT.code);
    this.flush();
  }

}
