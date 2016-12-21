package com.chinamobile.bcbsp.zookeeperserver;

import java.io.IOException;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.BSPZookeeper;
import com.chinamobile.bcbsp.thirdPartyInterface.Zookeeper.impl.BSPZookeeperImpl;

public class Zookeeper_Server implements Watcher {

	private static final int SESSION_TIMEOUT = 10000;
	private BSPZookeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);

	public BSPZookeeper connect() throws IOException, InterruptedException {
		zk = new BSPZookeeperImpl("192.168.1.3:2181", SESSION_TIMEOUT, this);
		this.zkWaitConnected(zk);
		return zk;
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}
	}

	public void zkWaitConnected(BSPZookeeper zk) {
		if (zk.equaltoState()) {
			try {
				this.connectedSignal.await();
			} catch (InterruptedException e) {
				throw new IllegalStateException(e);
			}
		}
	}

}
