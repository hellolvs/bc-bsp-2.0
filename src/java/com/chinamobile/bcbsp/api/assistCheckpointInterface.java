package com.chinamobile.bcbsp.api;

import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.util.BSPJob;

public interface assistCheckpointInterface {

	void init(BSPJob job);

	boolean writeAssCheckPoint(Path newPath, BSPJob job,
			Staff staff);
	
	String readAssCheckPoint(Path path,BSPJob job,Staff staff);
	

}
