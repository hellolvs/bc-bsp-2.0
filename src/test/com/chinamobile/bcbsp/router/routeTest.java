package com.chinamobile.bcbsp.router;

import static org.junit.Assert.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.chinamobile.bcbsp.partition.HashPartitioner;

public class routeTest {

	// private int numPartition =4;
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGetpartitionID() {
		String vertexID = "7";
		String vertexID2 = "66";
		HashMap<Integer, Integer> hashBucketToPartition = new HashMap<Integer, Integer>();

		hashBucketToPartition.put(0, 1);
		hashBucketToPartition.put(2, 1);
		hashBucketToPartition.put(1, 2);
		hashBucketToPartition.put(3, 2);

		HashMap<Integer, Integer> rangeroute = new HashMap<Integer, Integer>();
		rangeroute.put(50, 0);
		rangeroute.put(100, 1);

		routeparameter rp = new routeparameter();
		rp.setPartitioner(new HashPartitioner(2));
		route router = simplerouteFactory.createroute(rp);
		int partitionID = router.getpartitionID(new Text(vertexID));
		assertEquals((int) testID(vertexID), partitionID);
		System.out.println(partitionID);
		System.out.println(testID(vertexID));
		rp.setHashBucketToPartition(hashBucketToPartition);
		route routers = simplerouteFactory.createroute(rp);
		int PID = routers.getpartitionID(new Text(vertexID));
		int realPID = hashBucketToPartition.get((int) testIDs(vertexID));
		assertEquals(realPID, PID);

		rp.setHashBucketToPartition(null);
		rp.setRangeRouter(rangeroute);
		route routerss = simplerouteFactory.createroute(rp);
		int partitionID1 = routerss.getpartitionID(new Text(vertexID));
		int partitionID2 = routerss.getpartitionID(new Text(vertexID2));
		assertEquals(0, partitionID1);
		assertEquals(1, partitionID2);

	}

	public long testID(String vertexID) {

		int numPartition = 2;

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

	public long testIDs(String vertexID) {

		int numPartition = 4;

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
