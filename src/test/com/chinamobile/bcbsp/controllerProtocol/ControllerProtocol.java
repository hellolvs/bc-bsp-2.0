/**
 * CopyRight by Chinamobile
 *
 * ControllerProtocol.java
 */

package com.chinamobile.bcbsp.controllerProtocol;

import com.chinamobile.bcbsp.rpc.BSPRPCProtocolVersion;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;
import com.chinamobile.bcbsp.action.Directive;

import java.io.IOException;

/**
 * ControllerProtocol A new protocol for WorkerManagers communicate with
 * BSPController. This protocol paired with WorkerProtocl, let WorkerManagers
 * enroll with BSPController, so that BSPController can dispatch staffs to
 * WorkerManagers.
 *
 *
 *
 */
public interface ControllerProtocol extends BSPRPCProtocolVersion {

  /**
   * A WorkerManager register with its status to BSPController, which will
   * update WorkerManagers cache.
   *
   * @param status
   *        to be updated in cache.
   * @return true if successfully register with BSPController; false if fail.
   */
  boolean register(WorkerManagerStatus status) throws IOException;

  /**
   * A WorkerManager (periodically) reports task statuses back to the
   * BSPController.
   *
   * @param directive
   *        Directive
   * @return
   *        true or false
   */
  boolean report(Directive directive) throws IOException;

  /**
   * get system directory
   * @return
   *      system directory
   */
  String getSystemDir();

}
