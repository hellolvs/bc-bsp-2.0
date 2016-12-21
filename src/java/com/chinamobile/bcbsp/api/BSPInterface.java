/**
 * CopyRight by Chinamobile
 *
 * BSPInterface.java
 */

package com.chinamobile.bcbsp.api;

import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.util.StaffAttemptID;

import java.util.Iterator;

/**
 * BSPInterface Interface BSP defines the basic operations needed to implement
 * the BSP algorithm.
 *
 * @param <M>
 *
 */
public interface BSPInterface<M> {

  /**
   * Setup before invoking the
   * {@link com.chinamobile.bcbsp.staff.BSPInterface.compute} function. User can
   * make some preparations in this function.
   *
   * @param staff
   *        Staff
   */
  void setup(Staff staff);

  /**
   * Initialize before each super step. User can init some global variables for
   * each super step.
   *
   * @param context
   *        SuperStepContextInterface
   */
  void initBeforeSuperStep(SuperStepContextInterface context);
  
  /**
   * Initialize after each super step. User can save some global variables for
   * each super step.
   *
   * @param staffId
   *        StaffAttemptID
   */
  void initAfterSuperStep(StaffAttemptID staffId);

  /**
   * A user defined function for programming in the BSP style. Applications can
   * use the {@link com.chinamobile.bcbsp.bsp.WorkerAgent} to handle the
   * communication and synchronization between processors.
   *
   * @param messages
   *        Iterator
   * @param context
   *        BSPStaffContextInterface
   * @throws Exception
   */
  void compute(Iterator<M> messages, BSPStaffContextInterface context)
      throws Exception;

  /**
   * Cleanup after finishing the staff. User can define the specific work in
   * this function.
   *
   * @param staff
   *        Staff
   */
  void cleanup(Staff staff);

}
