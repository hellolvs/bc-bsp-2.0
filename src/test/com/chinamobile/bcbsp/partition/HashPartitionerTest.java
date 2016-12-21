package com.chinamobile.bcbsp.partition;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.junit.Ignore;
import org.junit.Test;

import com.chinamobile.bcbsp.Constants;

public class HashPartitionerTest {
	private int numPartition = 2;

	@Test
	public void testGetPartitionID() {

		HashPartitioner<String> hashPartition = new HashPartitioner<String>();
		hashPartition.setNumPartition(numPartition);
		File file = new File("D:\\work\\file2.txt");

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
				line++;
				int pid = -1;
				if (vertexID != null) {
					pid = hashPartition.getPartitionID(vertexID);
				}
				System.out.println("PartitionID  " + pid);
				long expectedpid = -1;
				expectedpid = testID(vertexID);

				assertEquals(expectedpid, pid);
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

	public long testID(String vertexID) {

		String id = vertexID;
		MessageDigest md5 = null;
		if (md5 == null) {
			try {
				md5 = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {

				throw new IllegalStateException("no md5 algorythm found");
			}
		}
		md5.reset();
		md5.update(id.getBytes());
		byte[] bKey = md5.digest();
		long hashcode = ((long) (bKey[3] & 0xFF) << 24)
				| ((long) (bKey[2] & 0xFF) << 16)
				| ((long) (bKey[1] & 0xFF) << 8) | (long) (bKey[0] & 0xFF);
		int result = (int) (hashcode % numPartition);
		result = (result < 0 ? result + numPartition : result);
		return result;
	}

}
