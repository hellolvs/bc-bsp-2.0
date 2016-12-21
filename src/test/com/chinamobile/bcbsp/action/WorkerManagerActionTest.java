package com.chinamobile.bcbsp.action;

import static org.junit.Assert.*;


import org.junit.Test;


import com.chinamobile.bcbsp.action.LaunchStaffAction;
import com.chinamobile.bcbsp.action.WorkerManagerAction;
import com.chinamobile.bcbsp.action.WorkerManagerAction.ActionType;




public class WorkerManagerActionTest {
	
	
	@Test
	public void testCreateAction() {

		WorkerManagerAction action = WorkerManagerAction
				.createAction(ActionType.LAUNCH_STAFF);
		WorkerManagerAction WMaction = new LaunchStaffAction();
		assertEquals(WMaction.getActionType(), action.getActionType());
	}

}
