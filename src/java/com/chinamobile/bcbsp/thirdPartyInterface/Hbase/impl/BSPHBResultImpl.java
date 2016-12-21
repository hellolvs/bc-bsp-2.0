
package com.chinamobile.bcbsp.thirdPartyInterface.Hbase.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.Hbase.BSPHBResult;

import org.apache.hadoop.hbase.client.Result;

/**
 *
 * BSPHBResultImpl A concrete class that implements interface BSPHBResult.
 *
 */
public class BSPHBResultImpl extends Result implements BSPHBResult {
  /** set result */
  private Result result = null;
}
