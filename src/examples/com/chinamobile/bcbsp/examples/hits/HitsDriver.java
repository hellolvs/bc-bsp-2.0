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
package com.chinamobile.bcbsp.examples.hits;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.io.KeyValueBSPFileInputFormat;
import com.chinamobile.bcbsp.io.TextBSPFileOutputFormat;
import com.chinamobile.bcbsp.util.BSPJob;

import org.apache.hadoop.fs.Path;

/**
* PageRnak BSP driver.
*/
public class HitsDriver {
 /**
  * main method.
  * @param args command parameter
  * @throws Exception
  */
 public static void main(String[] args) throws Exception {
   if (args.length < 3) {
     System.out
         .println("Usage: <nSupersteps>  <FileInputPath>  <FileOutputPath>  " +
           "<SplitSize(MB)> <PartitionNum> <HashBucketNum>");
     System.exit(-1);
   }
   // Set the base configuration for the job
   System.out.println("Hits-test V4.0!");
   BSPConfiguration conf = new BSPConfiguration();
   BSPJob bsp = new BSPJob(conf, HitsDriver.class);
   bsp.setJobName("HITS-ByteArray");
   bsp.setNumSuperStep(Integer.parseInt(args[0]));
   bsp.setPartitionType(Constants.PARTITION_TYPE.HASH);
   bsp.setPriority(Constants.PRIORITY.NORMAL);
   // /FOR TEST THE FOLLOWING ONE IS MODIFIED
   if (args.length > 3) {
     bsp.setSplitSize(Integer.valueOf(args[3]));
   }
   if (args.length > 4) {
     bsp.setNumPartition(Integer.parseInt(args[4]));
   }
   if (args.length > 5) {
     bsp.setHashBucketNumber(Integer.parseInt(args[5]));
   }
//   if (args.length > 6) {
//     bsp.setOutgoingEdgeNum(Long.valueOf(args[6]));
//   }
   // Set the BSP.class
   bsp.setBspClass(HitsBSPNew.class);
   // Set communication version
   bsp.setRecordParse(HitsRP.class);
   bsp.setVertexClass(SVertex.class);
   bsp.setEdgeClass(SEdge.class);
   bsp.setInputFormatClass(KeyValueBSPFileInputFormat.class);
   bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
   
   bsp.setMessageClass(HitsMessage.class);
   // Set the InputPath and OutputPath
   KeyValueBSPFileInputFormat.addInputPath(bsp, new Path(args[1]));
   TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[2]));
   bsp.setReceiveCombinerSetFlag(false);
   bsp.setMaxProducerNum(38);
   bsp.setCommunicationOption(Constants.RPC_BYTEARRAY_VERSION);
   // Set the graph data implementation version as disk version.
   bsp.setGraphDataVersion(bsp.BYTEARRAY_VERSION);
  // bsp.setCombineFlag(false);
   bsp.waitForCompletion(true);
 }
}
