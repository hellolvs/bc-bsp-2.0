
package com.chinamobile.bcbsp.thirdPartyInterface.HDFS;

import java.io.File;
import java.io.IOException;

/**
 *
 * BSPFileUtil An interface that encapsulates FileUtil.
 *
 */
public interface BSPFileUtil {

  /**
   *  Delete a directory and all its contents.
   *  If we return false, the directory may be partially-deleted.
   * @param f
   *        file
   * @return
   *        true or false
   * @throws IOException
   */
  boolean fullyDelete(File f) throws IOException;

  /**
   * Convert a os-native filename to a path that works for the shell.
   * @param file
   *        The filename to convert
   * @return
   *        The unix pathname
   * @throws IOException
   */
  String makeShellPath(File file) throws IOException;

  /**
   *  Change the permissions on a filename.
   * @param filename
   *        the name of the file to change
   * @param perm
   *        the permission string
   * @return
   *        the exit code from the command
   * @throws IOException
   * @throws InterruptedException
   */
  int chmod(String filename, String perm) throws IOException,
      InterruptedException;
}
