package com.chinamobile.bcbsp.api;

import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.util.BSPJob;

public abstract class assistCheckpoint implements assistCheckpointInterface{

	public void init(BSPJob job) {
		// TODO Auto-generated method stub
		
	}

	public boolean writeAssCheckPoint(Path newPath,
			BSPJob job, Staff staff) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public String readAssCheckPoint(Path path,BSPJob job, Staff staff){
		return null;
	}

}
