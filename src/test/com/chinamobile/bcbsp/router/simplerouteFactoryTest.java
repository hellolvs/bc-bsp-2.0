package com.chinamobile.bcbsp.router;

import static org.junit.Assert.*;

import java.util.HashMap;

import org.junit.Test;

import com.chinamobile.bcbsp.partition.HashPartitioner;

public class simplerouteFactoryTest {

	@Test
	public void testCreateroute() {

		HashMap<Integer, Integer> hashBucketToPartition = new HashMap<Integer, Integer>();

		routeparameter rp = new routeparameter();

		rp.setPartitioner(new HashPartitioner(2));

		route router = simplerouteFactory.createroute(rp);

		assertTrue(router instanceof HashRoute);

		rp.setHashBucketToPartition(hashBucketToPartition);

		route routers = simplerouteFactory.createroute(rp);

		assertTrue(routers instanceof BalancerHashRoute);
		rp.setHashBucketToPartition(null);
		rp.setRangeRouter(hashBucketToPartition);
		route routerss = simplerouteFactory.createroute(rp);
		assertTrue(routerss instanceof RangeRoute);
	}

}
