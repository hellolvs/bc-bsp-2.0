package com.chinamobile.bcbsp.action;

import static org.junit.Assert.*;


import java.util.ArrayList;


import org.junit.Test;

import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.action.LaunchStaffAction;
import com.chinamobile.bcbsp.action.WorkerManagerAction;



public class DirectiveTest {

   
	@Test
	public void testAddAction() {
		ArrayList<WorkerManagerAction> actions = new ArrayList<WorkerManagerAction>();
		Directive dir = new Directive();
		dir.setActionList(actions);
		WorkerManagerAction WMaction = new LaunchStaffAction();
		dir.addAction(WMaction);
		assertEquals(true, dir.getActionList().contains(WMaction));

	}
	
}
    




