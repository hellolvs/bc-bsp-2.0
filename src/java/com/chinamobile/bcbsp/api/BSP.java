/**
 * CopyRight by Chinamobile
 *
 * BSP.java
 */

package com.chinamobile.bcbsp.api;

import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * BSP This class provides an abstract implementation of the BSP interface.
 *
 * @param <M>
 *
 */
public abstract class BSP<M> implements BSPInterface<M> {

  /**
   * The default implementation of setup does nothing.
   *
   * @param staff
   *        Staff
   */
  public void setup(Staff staff) {

  }

  /**
   * The default implementation of initBeforeSuperStep does nothing.
   *
   * @param context
   *        SuperStepContextInterface
   */
  @Override
  public void initBeforeSuperStep(SuperStepContextInterface context) {

  }

    /**
   * The default implementation of initAfterSuperStep does nothing.
   *
   * @param sid
   *        StaffAttemptID
   */
  @Override
  public void initAfterSuperStep(StaffAttemptID sid) { 
  
  }
  
  /**
   * The default implementation of cleanup does nothing.
   *
   * @param staff
   *        Staff
   */
  public void cleanup(Staff staff) {

  }
}
