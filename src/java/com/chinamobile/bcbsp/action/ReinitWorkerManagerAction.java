/**
 * CopyRight by Chinamobile
 *
 * ReinitWorkerManagerAction.java
 */

package com.chinamobile.bcbsp.action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ReinitWorkerManagerAction Represents a directive from the
 * {@link com.chinamobile.bcbsp.bsp.BSPController} to the
 * {@link com.chinamobile.bcbsp.bsp.WorkerManager} to reinitialize itself.
 *
 *
 *
 */
class ReinitWorkerManagerAction extends WorkerManagerAction {

  /**
   *  constructor
   */
  public ReinitWorkerManagerAction() {
    super(ActionType.REINIT_WORKERMANAGER);
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }

}
