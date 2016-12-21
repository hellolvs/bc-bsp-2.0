package com.chinamobile.bcbsp.rpcserver;

public class WorkerAgentServer {

	@SuppressWarnings({ "static-access", "resource" })
	public static void main(String[] args) throws InterruptedException {

		workerAgent_Server r = new workerAgent_Server();
		while (true) {
			if (r.getFlag() == 0) {
				Thread.sleep(1000000);
			} else {
				break;
			}
		}

	}

}
