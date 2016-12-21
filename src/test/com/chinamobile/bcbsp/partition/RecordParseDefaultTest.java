package com.chinamobile.bcbsp.partition;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.examples.PRVertex;
import com.chinamobile.bcbsp.util.BSPJob;

public class RecordParseDefaultTest {

	private File file = new File("D:\\work\\file2.txt");
	private RecordParseDefault recordParse = new RecordParseDefault();
	private BSPJob job = new BSPJob(new BSPConfiguration(), 2);

	@Ignore
	public void testInit() {
		fail("Not yet implemented");
	}

	@SuppressWarnings({ "unused", "rawtypes" })
	@Test
	public void testRecordParse() {

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			while (((tempString = reader.readLine()) != null)
					&& (tempString.length() > 0)) {

				System.out.println("line " + line + ": " + tempString);

				String[] kvStrings = tempString.split(Constants.KV_SPLIT_FLAG);
				String[] vertexinfo = kvStrings[0].split(Constants.SPLIT_FLAG);
				String vertexID = vertexinfo[0];
				job.setVertexClass(PRVertex.class);
				recordParse.init(job);
				// String expectedVertexID=recordParse.getVertexID(new
				// Text(kvStrings[0])).toString();
				Vertex vertex = recordParse.recordParse(kvStrings[0],
						kvStrings[1]);
				line++;

				assertEquals(tempString, vertex.intoString());
			}

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}

	}

	@Test
	public void testGetVertexID() {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			while (((tempString = reader.readLine()) != null)
					&& (tempString.length() > 0)) {

				System.out.println("line " + line + ": " + tempString);

				String[] kvStrings = tempString.split(Constants.KV_SPLIT_FLAG);
				String[] vertexinfo = kvStrings[0].split(Constants.SPLIT_FLAG);
				String vertexID = vertexinfo[0];
				String expectedVertexID = recordParse.getVertexID(
						new Text(kvStrings[0])).toString();
				line++;

				assertEquals(expectedVertexID, vertexID);
			}

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
	}

}
