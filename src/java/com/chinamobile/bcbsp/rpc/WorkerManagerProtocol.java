/**
 * CopyRight by Chinamobile
 *
 * WorkerManagerProtocol.java
 */

package com.chinamobile.bcbsp.rpc;

import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.util.BSPJobID;

import java.io.IOException;

/**
 * WorkerManagerProtocol A protocol for BSPController talks to WorkerManager.
 * This protocol allow BSPController dispatch tasks to a WorkerManager.
 *
 *
 *
 */
public interface WorkerManagerProtocol extends BSPRPCProtocolVersion {

  /**
   * Instruct WorkerManager performaning tasks.
   *
   * @param directive
   *        instructs a WorkerManager performing necessary execution.
   * @param jobId
   *        BSPJobID
   * @param isRecovery
   *        boolean
   * @param changeWorkerState
   *        boolean
   * @param failCounter
   *        int
   * @return
   *        true
   * @throws IOException
   */
  boolean dispatch(BSPJobID jobId, Directive directive, boolean isRecovery,
      boolean changeWorkerState, int failCounter) throws IOException;

  /**
   * Removes all of the elements from this list.  The list will
   * be empty after this call returns.
   */
  void clearFailedJobList();

  /**
   * Appends the specified element to the end of this list.
   * @param jobId
   *        BSPJobID
   */
  void addFailedJob(BSPJobID jobId);

  /**
   *  Returns the number of elements in this list.
   * @return
   *        the number of elements in this list.
   */
  int getFailedJobCounter();
}
