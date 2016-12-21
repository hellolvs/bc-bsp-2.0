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

package com.chinamobile.bcbsp.fault.storage;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.CommunicationFactory;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.comm.GraphStaffHandler;
import com.chinamobile.bcbsp.comm.IMessage;
import com.chinamobile.bcbsp.graph.GraphDataFactory;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.io.TextBSPFileOutputFormat;
import com.chinamobile.bcbsp.io.db.TableOutputFormat;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPHdfs;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPoutHdfs;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPHdfsImpl;
import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl.BSPoutHdfsImpl;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * write and read the checkpoint for fault tolerance and staff migrate.
 * @author hadoop
 */
public class Checkpoint {
  /**handle log information in checkpoint class*/
  private static final Log LOG = LogFactory.getLog(Checkpoint.class);
  /**vertex class handle*/
  private Class<? extends Vertex<?, ?, ?>> vertexClass;
  /**
   * checkpoint construct method,get the job vertex class.
   * @param job
   *        job to checkpoint.
   */
  public Checkpoint(BSPJob job) {
    vertexClass = job.getVertexClass();
  }

  /**
   * Write Check point
   * @param graphData
   *        graphdata to checkpoint
   * @param writePath
   *        checkpoint write path
   * @param job
   *        job to checkpoint
   * @param staff
   *        staff to checkpoint
   * @return boolean Note it should be modified , some detailed opts to be
   *         sealed into graph data interface and should not be exposed outside.
   */
  @SuppressWarnings("unchecked")
  public boolean writeCheckPoint(GraphDataInterface graphData, Path writePath,
      BSPJob job, Staff staff) throws IOException {
    LOG.info("The init write path is : " + writePath.toString());
    try {
      OutputFormat outputformat = null;
      if (job.getCheckpointType().equals("HBase")) {
        createTable(job, staff.getStaffAttemptId());
        outputformat = (OutputFormat) ReflectionUtils.newInstance( TableOutputFormat.class, job.getConf());
        job.getConf().set(TableOutputFormat.OUTPUT_TABLE, staff.getStaffAttemptId().toString());
      } else if (job.getCheckpointType().equals("HDFS")){
        outputformat = (OutputFormat) ReflectionUtils.newInstance(TextBSPFileOutputFormat.class, job.getConf());
      }
      outputformat.initialize(job.getConf());
      RecordWriter output = outputformat.getRecordWriter(job,
          staff.getStaffAttemptId(), writePath);
      graphData.saveAllVertices(output);
      output.close(job);
    } catch (Exception e) {
      LOG.error("Exception has happened and been catched!", e);
      return false;
    }
    return true;
  }

  /**
   * Read Check point
   * @param readPath
   *        checkpoint readpath
   * @param job
   *        job to read the checkpoint
   * @param staff
   *        staff to get the graphdata
   * @return checkpoint graphdata
   */
  @SuppressWarnings("unchecked")
  public GraphDataInterface readCheckPoint(Path readPath, BSPJob job,
      Staff staff) {
    GraphDataInterface graphData = null;
    GraphDataFactory graphDataFactory = staff.getGraphDataFactory();
    int version = job.getGraphDataVersion();
    graphData = graphDataFactory.createGraphData(version, staff);
    Vertex vertexTmp = null;
    String s = null;
    if (job.getCheckpointType().equals("HBase")) {
      HTablePool pool = new HTablePool(job.getConf(), 1000);
      HTable table = (HTable) pool.getTable(staff.getStaffAttemptId().toString());
      try {
        ResultScanner rs = table.getScanner(new Scan());
        for (Result r : rs) {       
          KeyValue[] keyValue = r.raw();
          s = new String(r.getRow()) + Constants.KV_SPLIT_FLAG + new String(keyValue[0].getValue());
          if (s != null) {
            try {
              vertexTmp = this.vertexClass.newInstance();
              vertexTmp.fromString(s);
            } catch (Exception e) {
              throw new RuntimeException("[Checkpoint] caught: ", e);
            }
            if (vertexTmp != null) {
              LOG.info("vertexTmp = " + vertexTmp);
              graphData.addForAll(vertexTmp);
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      
    } else if(job.getCheckpointType().equals("HDFS")){
      LOG.info("The init read path is : " + readPath.toString());
      String uri = readPath.toString() + "/"
          + staff.getStaffAttemptId().toString() + "/checkpoint.cp";
      LOG.info("uri: " + uri);
      // Configuration conf = new Configuration();
      // alter by gtt
      BSPHdfs HdfsCheckpoint = new BSPHdfsImpl();
      InputStream in = null;
      BufferedReader bis = null;
      try {
        // FileSystem fs = FileSystem.get(URI.create(uri), conf);
        // in = fs.open(new Path(uri));
        in = HdfsCheckpoint.hdfsCheckpoint(uri, HdfsCheckpoint.getConf());
        bis = new BufferedReader(new InputStreamReader(new BufferedInputStream(
            in)));
        s = bis.readLine();
        while (s != null) {
          try {
            vertexTmp = this.vertexClass.newInstance();
            vertexTmp.fromString(s);
          } catch (Exception e) {
            // LOG.error("[Checkpoint] caught: ", e);
            throw new RuntimeException("[Checkpoint] caught: ", e);
          }
          if (vertexTmp != null) {
            graphData.addForAll(vertexTmp);
          }
          s = bis.readLine();
        }
        graphData.finishAdd();
        bis.close();
      } catch (IOException e) {
        // LOG.error("Exception has happened and been catched!", e);
        throw new RuntimeException("Exception has happened and been catched!",
            e);
      }
    }
    return graphData;
  }
  
  public GraphDataInterface readIncCheckPoint(Path oriPath, Path readPath, BSPJob job,
	      Staff staff) {
	    GraphDataInterface graphData = null;
	    GraphDataFactory graphDataFactory = staff.getGraphDataFactory();
	    int version = job.getGraphDataVersion();
	    graphData = graphDataFactory.createGraphData(version, staff);
	    Vertex vertexTmp = null;
	    String s = null;
	    String oris = null;
	    if (job.getCheckpointType().equals("HBase")) {
	      HTablePool pool = new HTablePool(job.getConf(), 1000);
	      HTable table = (HTable) pool.getTable(staff.getStaffAttemptId().toString());
	      try {
	        ResultScanner rs = table.getScanner(new Scan());
	        for (Result r : rs) {       
	          KeyValue[] keyValue = r.raw();
	          s = new String(r.getRow()) + Constants.KV_SPLIT_FLAG + new String(keyValue[0].getValue());
	          if (s != null) {
	            try {
	              vertexTmp = this.vertexClass.newInstance();
	              vertexTmp.fromString(s);
	            } catch (Exception e) {
	              throw new RuntimeException("[Checkpoint] caught: ", e);
	            }
	            if (vertexTmp != null) {
	              LOG.info("vertexTmp = " + vertexTmp);
	              graphData.addForAll(vertexTmp);
	            }
	          }
	        }
	      } catch (IOException e) {
	        e.printStackTrace();
	      }
	      
	    } else if(job.getCheckpointType().equals("HDFS")){
	      LOG.info("The init read path is : " + readPath.toString());
	      String uri = readPath.toString() + "/"
	          + staff.getStaffAttemptId().toString() + "/checkpoint.cp";
	      String oriuri = oriPath.toString() + "/" + staff.getStaffAttemptId().toString() + "/checkpoint.cp";
	      LOG.info("uri: " + uri);
	      // Configuration conf = new Configuration();
	      // alter by gtt
	      BSPHdfs HdfsCheckpoint = new BSPHdfsImpl();
	      InputStream in = null;
	      //added for read the origraphdata
	      InputStream oriin = null;
	      BufferedReader bis = null;
	      BufferedReader oribis = null;
	      try {
	        // FileSystem fs = FileSystem.get(URI.create(uri), conf);
	        // in = fs.open(new Path(uri));
	        in = HdfsCheckpoint.hdfsCheckpoint(uri, HdfsCheckpoint.getConf());
	        oriin = HdfsCheckpoint.hdfsCheckpoint(oriuri, HdfsCheckpoint.getConf());
	        bis = new BufferedReader(new InputStreamReader(new BufferedInputStream(
	            in)));
	        oribis = new BufferedReader(new InputStreamReader(new BufferedInputStream(
	                oriin)));
	        s = bis.readLine();
	        oris = oribis.readLine();
	        String[] orisSet = oris.split(Constants.KV_SPLIT_FLAG);
	        if(orisSet.length==2){
	        	s = s +Constants.KV_SPLIT_FLAG+orisSet[1];
	        }
	        while (s != null) {
	          try {
	            vertexTmp = this.vertexClass.newInstance();
	            vertexTmp.fromString(s);
	           
	          } catch (Exception e) {
	            // LOG.error("[Checkpoint] caught: ", e);
	            throw new RuntimeException("[Checkpoint] caught: ", e);
	          }
	          if (vertexTmp != null) {
	            graphData.addForAll(vertexTmp);
	          }
	          s = bis.readLine();
	          if(s!=null){
	          oris = oribis.readLine();
	          String orisTmp = oris.split(Constants.KV_SPLIT_FLAG)[1];
	          s = s + Constants.KV_SPLIT_FLAG + orisTmp;
	          }
	        }
	        graphData.finishAdd();
	        bis.close();
	        oribis.close();
	      } catch (IOException e) {
	        // LOG.error("Exception has happened and been catched!", e);
	        throw new RuntimeException("Exception has happened and been catched!",
	            e);
	      }
	    }
	    return graphData;
	  }

  /**
   * write the message and graphdata information for
   * migrate slow staff.
   * @param communicator
   *        message information need to backup
   * @param graphData
   *        graphdata to backup
   * @param writePath
   *        backup write path
   * @param job
   *        job to backup
   * @param staff
   *        staff to backup
   * @return write result if success and nothing to write true
   *         not false
   * @throws IOException
   *         exceptions when write messages
   * @author liuzhicheng
   */
  public boolean writeMessages(Path writePath, BSPJob job, GraphStaffHandler graphStaffHandler,
      Staff staff,ConcurrentLinkedQueue<String> messagesQueue)
      throws IOException, InterruptedException {
    try{

        LOG.info("The init write path is : " + writePath.toString());
        BSPHdfs HDFSCheckpoint = new BSPHdfsImpl();
        BSPoutHdfs OUT = new BSPoutHdfsImpl();
        OUT.fsDataOutputStream(writePath, HDFSCheckpoint.getConf());
        StringBuffer sb = new StringBuffer();
        
        ConcurrentLinkedQueue<String> messages = messagesQueue;
        Iterator<String> it = messages.iterator();

        while(it.hasNext()){
        	String message = it.next();
        	//LOG.info("Write messages "+message);
        	sb.append(message);
        	sb.append("\n");
        }

        OUT.writeBytes(sb.toString());
        OUT.flush();
        OUT.close();
       // return true;
    }catch (IOException e){
    	LOG.error("Exception has happened and been catched!", e);
        return false;
        }
    return true;

  }

  /**
   * read message information
   * @param readPath
   *        message backup to read
   * @param job
   *        need to read the message backup
   * @param staff
   *        read the message backup
   * @return read successfully true
   *         not false.
   */
  public Map<String, LinkedList<IMessage>> readMessages(Path readPath,
      BSPJob job, Staff staff) {
    LOG.info("The init read path is : " + readPath.toString());
    String uri = readPath.toString();
    InputStream in = null;
    //boolean exist = false;
    BufferedReader bis = null;
    Map<String, LinkedList<IMessage>> incomedMessages = new
        HashMap<String, LinkedList<IMessage>>();
    CommunicationFactory.setMessageClass(job.getMessageClass());
    try {
      BSPHdfs BSPRead = new BSPHdfsImpl();
      //exist = BSPRead.exists(BSPRead.newPath(uri));
      in = BSPRead.hdfsCheckpoint(uri, BSPRead.getConf());
      bis = new BufferedReader(new InputStreamReader(
          new BufferedInputStream(in)));
      String s = bis.readLine();
      if(s == null){
      LOG.info("message file is empty! ");
      }
      while (s != null) {
        try {
          String[] msgInfos = s.split(Constants.MESSAGE_SPLIT);
          if(msgInfos.length>0){
          //LOG.info("message is not empty!" +msgInfos.length);
          }
          String vertexID = msgInfos[0];
          String[] msgs = msgInfos[1].split(Constants.SPACE_SPLIT_FLAG);
          LinkedList<IMessage> list = new LinkedList<IMessage>();
          for (int i = 0; i < msgs.length; i++) {
        	  String[] msgss = msgs[i].split(Constants.SPLIT_FLAG);
        	  IMessage msg = CommunicationFactory.createPagerankMessage();
        	  if(msgss.length > 0){
        		  //biyahui noted
        		  //msg.setMessageId(Integer.parseInt(msgss[0]));
        		  //msg.setContent(Float.parseFloat(msgss[1]));
        		  String message=msgss[0]+Constants.SPLIT_FLAG+msgss[1];
        		  msg.fromString(message);
        	  }
            list.add(msg);
          }
          incomedMessages.put(vertexID, list);
        } catch (Exception e) {
          //LOG.error("[Checkpoint] caught: ", e);
          throw new RuntimeException("[Checkpoint] caught: ", e);
        }
        s = bis.readLine();
      }
      bis.close();
    } catch (IOException e) {
      //LOG.error("Exception has happened and been catched!", e);
      throw new RuntimeException("Exception has happened and been catched!", e);
    }
    return incomedMessages;
  }
  
  private void createTable(BSPJob job, StaffAttemptID staffId){
    LOG.info("create hbase table");
    Configuration conf = HBaseConfiguration.create();
//    conf.addResource(new Path("/usr/local/termite/bc-bsp-1.0/conf/bcbsp-site.xml")); 
    conf.set("hbase.zookeeper.property.clientPort", job.getConf().get(Constants.ZOOKEPER_CLIENT_PORT));  
    conf.set("hbase.zookeeper.quorum", "master");  
    conf.set("hbase.master", "master:60000");  

    String tableName = staffId.toString();
    String columnFamilyName = "BorderNode";
        
    try {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }          
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        descriptor.addFamily(new HColumnDescriptor(columnFamilyName));
        admin.createTable(descriptor); 
        admin.close();
    } catch (MasterNotRunningException e1) {
        e1.printStackTrace();
    } catch (ZooKeeperConnectionException e1) {
        e1.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }
  }
  /**
   * feng added for checkpoint to write the Increment part
   * @param graphData
   * @param writePath
   * @param job
   * @param staff
   * @return
   * @throws IOException
   */
  public boolean writeIncrementCheckPoint(GraphDataInterface graphData, Path writePath,
	      BSPJob job, Staff staff) throws IOException {
	    LOG.info("The init write path is : " + writePath.toString());
	    try {
	      OutputFormat outputformat = null;
	      if (job.getCheckpointType().equals("HBase")) {
	        createTable(job, staff.getStaffAttemptId());
	        outputformat = (OutputFormat) ReflectionUtils.newInstance( TableOutputFormat.class, job.getConf());
	        job.getConf().set(TableOutputFormat.OUTPUT_TABLE, staff.getStaffAttemptId().toString());
	      } else if (job.getCheckpointType().equals("HDFS")){
	        outputformat = (OutputFormat) ReflectionUtils.newInstance(TextBSPFileOutputFormat.class, job.getConf());
	      }
	      outputformat.initialize(job.getConf());
	      RecordWriter output = outputformat.getRecordWriter(job,
	          staff.getStaffAttemptId(), writePath);
	      graphData.saveAllVertices(output,true);
	      output.close(job);
	    } catch (Exception e) {
	      LOG.error("Exception has happened and been catched!", e);
	      return false;
	    }
	    return true;
	  }
}