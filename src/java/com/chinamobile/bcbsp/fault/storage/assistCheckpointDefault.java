package com.chinamobile.bcbsp.fault.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.api.*;
import com.chinamobile.bcbsp.util.BSPJob;

public class assistCheckpointDefault extends assistCheckpoint{
	  /**handle log file in the class*/
	  private static final Log LOG = LogFactory.getLog(assistCheckpointDefault.class);
	  /**AggValueCheckpoint construct method
	   * @param job
	   *        job's aggregate values to write.
	   */
	  public assistCheckpointDefault() {
	  }
	  public void init(BSP job){
		  
	  }
}
