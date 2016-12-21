package com.chinamobile.bcbsp.examples.lpclustering;

import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.io.KeyValueBSPFileInputFormat;
import com.chinamobile.bcbsp.io.TextBSPFileOutputFormat;
import com.chinamobile.bcbsp.util.BSPJob;

public class LPClusterDriver {
	/**
	   * main method.
	   * @param args command parameter
	   * @throws Exception
	   */
	  public static void main(String[] args) throws Exception {
	    if (args.length < 3) {
	      System.out
	          .println("Usage: <nSupersteps>  <FileInputPath>  <FileOutputPath>  " +
	            "<SplitSize(MB)> <PartitionNum> <HashBucketNum> <OutgoingNum>");
	      System.exit(-1);
	    }
	    // Set the base configuration for the job
	    BSPConfiguration conf = new BSPConfiguration();
	    BSPJob bsp = new BSPJob(conf, LPClusterDriver.class);
	    bsp.setJobName("LPAClustering-ByteArray");
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
	    if (args.length > 6) {
	      bsp.setOutgoingEdgeNum(Long.valueOf(args[6]));
	    }
	    // Set the BSP.class
	    bsp.setBspClass(BSPLPCluster.class);
	    // Set communication version
		bsp.setVertexClass(LPClusterVertex.class);
	    bsp.setEdgeClass(LPClusterEdge.class);
	    bsp.setInputFormatClass(KeyValueBSPFileInputFormat.class);
	    bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
	    bsp.setMessageClass(LPClusterMessage.class);
	    // Set the InputPath and OutputPath
	    KeyValueBSPFileInputFormat.addInputPath(bsp, new Path(args[1]));
	    TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[2]));
	    bsp.setReceiveCombinerSetFlag(false);
	    bsp.setMaxProducerNum(38);
	    bsp.setCommunicationOption(Constants.RPC_BYTEARRAY_VERSION);
	    // Set the graph data implementation version as disk version.
	    bsp.setGraphDataVersion(bsp.BYTEARRAY_VERSION);
	    bsp.waitForCompletion(true);
	  }
}
