package com.chinamobile.bcbsp.fault.browse;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

import com.chinamobile.bcbsp.fault.storage.Fault;

public class BrowseTest {
	class BrowseFortest extends Browse {
		void get() {
			this.sortrecords = new ArrayList<Fault>();
			Fault f = new Fault();
			f.setLevel(Fault.Level.CRITICAL);
		}
	}

	@Before
	public void setUp() throws Exception {
		Browse br;
		br = new Browse();
		br.setBufferedTime(30);
	}

	@Test
	public void testRetrieveByLevel() {
		Browse br = new BrowseFortest();
		List<Fault> testresult11 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult11.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult11);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByLevel", testresult11, br.retrieveByLevel());
	}

	@Test
	public void testRetrieveByPosition() {
		Browse br = new BrowseFortest();
		List<Fault> testresult21 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult21.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult21);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByPosition", testresult21,
				br.retrieveByPosition());
	}

	@Test
	public void testRetrieveByType() {
		Browse br = new BrowseFortest();
		List<Fault> testresult31 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult31.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult31);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByType", testresult31, br.retrieveByType());
	}

	@Test
	public void testRetrieveByTime() {
		Browse br = new BrowseFortest();
		List<Fault> testresult41 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult41.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult41);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByTime", testresult41, br.retrieveByTime());
	}

	@Test
	public void testRetrieveByLevelInt() {
		Browse br = new BrowseFortest();
		List<Fault> testresult12 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult12.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult12);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByLevelInt", testresult12,
				br.retrieveByLevel());
	}

	@Test
	public void testRetrieveByPositionInt() {
		Browse br = new BrowseFortest();
		List<Fault> testresult22 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult22.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult22);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByPositionInt", testresult22,
				br.retrieveByPosition());
	}

	@Test
	public void testRetrieveByTypeInt() {
		Browse br = new BrowseFortest();
		List<Fault> testresult32 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult32.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult32);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByTypeInt", testresult32, br.retrieveByType());
	}

	@Test
	public void testRetrieveByTimeInt() {
		Browse br = new BrowseFortest();
		List<Fault> testresult42 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult42.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult42);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByTimeInt", testresult42, br.retrieveByTime());
	}

	@Test
	public void testRetrieveByLevelString() {
		Browse br = new BrowseFortest();
		List<Fault> testresult13 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult13.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult13);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByLevelString", testresult13,
				br.retrieveByLevel());
	}

	@Test
	public void testRetrieveByPositionString() {
		Browse br = new BrowseFortest();
		List<Fault> testresult23 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult23.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult23);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByPositionString", testresult23,
				br.retrieveByPosition());
	}

	@Test
	public void testRetrieveByTypeString() {
		Browse br = new BrowseFortest();
		List<Fault> testresult33 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult33.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult33);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByTypeString", testresult33,
				br.retrieveByType());
	}

	@Test
	public void testRetrieveByTimeString() {
		Browse br = new BrowseFortest();
		List<Fault> testresult43 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult43.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult43);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByTimeInt", testresult43, br.retrieveByTime());
	}

	@Test
	public void testRetrieveByLevelStringInt() {
		Browse br = new BrowseFortest();
		List<Fault> testresult14 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult14.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult14);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByLevelStringInt", testresult14,
				br.retrieveByLevel());

	}

	@Test
	public void testRetrieveByPositionStringInt() {
		Browse br = new BrowseFortest();
		List<Fault> testresult24 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult24.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult24);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByPositionStringInt", testresult24,
				br.retrieveByPosition());
	}

	@Test
	public void testRetrieveByTypeStringInt() {
		Browse br = new BrowseFortest();
		List<Fault> testresult44 = new ArrayList<Fault>();
		Fault f = new Fault();
		testresult44.add(f);
		ReadFaultlog readFaultlog = mock(ReadFaultlog.class);
		when(readFaultlog.read()).thenReturn(testresult44);
		br.readFaultlog = readFaultlog;

		assertEquals("testRetrieveByTimeStringInt", testresult44,
				br.retrieveByTime());
	}

}
