package com.chinamobile.bcbsp.fault.browse;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.TestUtil;
import com.chinamobile.bcbsp.fault.storage.Fault;

public class MessageServiceTest {

	MessageService ms;

	List<Fault> testresult;

	@Before
	public void setUp() throws IOException, InterruptedException {
		ms = new MessageService();
		ms.setCurrentPage(1);
		ms.setPageSize(10);
		testresult = new ArrayList<Fault>();

		try {

			TestUtil.invoke(ms, "setTotalList", testresult);
			TestUtil.invoke(ms, "getTotalList", testresult);

		} catch (Exception e) {

		}
	}

	@Test
	public void testGetCurrentPage() {
		assertEquals("testGetCurrentPage()", 1, ms.getCurrentPage());
	}

	@Test
	public void testGetPageSize() {
		assertEquals("testGetPageSize()", 10, ms.getPageSize());
	}

	@Test
	public void testGetTotalRows() {
		assertEquals("testGetTotalRows", testresult.size(), ms.getTotalRows());
	}

}
