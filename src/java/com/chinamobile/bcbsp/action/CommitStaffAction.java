/**
 * CopyRight by Chinamobile
 *
 * CommitStaffAction.java
 */

package com.chinamobile.bcbsp.action;

import com.chinamobile.bcbsp.util.StaffAttemptID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * CommitStaffAction
 *
 *
 *
 */
class CommitStaffAction extends WorkerManagerAction {
  /** State StaffAttemptID */
  private StaffAttemptID staffId;

  /**
   * constructor
   */
  public CommitStaffAction() {
    super(ActionType.COMMIT_STAFF);
    staffId = new StaffAttemptID();
  }

  /**
   * constructor
   * @param staffId
   *        StaffAttemptID
   */
  public CommitStaffAction(StaffAttemptID staffId) {
    super(ActionType.COMMIT_STAFF);
    this.staffId = staffId;
  }

  public StaffAttemptID getStaffID() {
    return staffId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    staffId.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    staffId.readFields(in);
  }
}
