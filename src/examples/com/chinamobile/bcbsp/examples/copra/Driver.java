package com.chinamobile.bcbsp.examples.copra;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.io.KeyValueBSPFileInputFormat;
import com.chinamobile.bcbsp.io.TextBSPFileOutputFormat;
import com.chinamobile.bcbsp.util.BSPJob;

public class Driver {
	
	public static void main(String[] args) {
		
		if(args.length != 5) {
			System.out.println("Usage: <FileInputPath> <FileOutputPath> <SuperStep> <MaxOverlap> <StaffNum>");
			System.exit(-1);
		}
	
		BSPConfiguration conf = new BSPConfiguration();
		conf.setInt("MaxOverlap", Integer.parseInt(args[3]));
		
		try {
		
			BSPJob bsp = new BSPJob(conf, Driver.class);
	
			bsp.setJobName("COPRA");
			bsp.setNumPartition(Integer.parseInt(args[4]));
			bsp.setNumSuperStep(Integer.parseInt(args[2]));
			bsp.setPartitionType(Constants.PARTITION_TYPE.HASH);
			bsp.setPriority(Constants.PRIORITY.NORMAL);
	
			bsp.setBspClass(C_BSP.class);
			bsp.setVertexClass(C_Vertex.class);
			bsp.setEdgeClass(C_Edge.class);
	
			bsp.setInputFormatClass(KeyValueBSPFileInputFormat.class);
			bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
	
			KeyValueBSPFileInputFormat.addInputPath(bsp, new Path(args[0]));
			TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[1]));
			
		//	bsp.registerAggregator(C_BSP.C_SUM, C_Aggregator.class, C_AggregateValue.class);
			bsp.completeAggregatorRegister();
			
			bsp.setSendThreshold(0);                     // can get the incoming messages quickly for asynchronized
	
			bsp.setCommunicationOption(Constants.RPC_VERSION);
			bsp.setGraphDataVersion(bsp.MEMORY_VERSION);
			bsp.setMessageQueuesVersion(bsp.MEMORY_VERSION);
		
			bsp.waitForCompletion(true);
		
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
}