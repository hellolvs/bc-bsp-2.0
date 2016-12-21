
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS.impl;

import com.chinamobile.bcbsp.thirdPartyInterface.HDFS.BSPFileUtil;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileUtil;

/**
 *
 * BSPFileUtilImpl A concrete class that implements interface BSPFileUtil.
 *
 */
public class BSPFileUtilImpl implements BSPFileUtil {

  @Override
  public boolean fullyDelete(File f) throws IOException {
    // TODO Auto-generated method stub
    return FileUtil.fullyDelete(f);

  }

  @Override
  public String makeShellPath(File file) throws IOException {
    // TODO Auto-generated method stub
    return FileUtil.makeShellPath(file);
  }

  @Override
  public int chmod(String filename, String perm) throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub
    return FileUtil.chmod(filename, perm);
  }

}
