/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.chinamobile.bcbsp.util;

import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sun.misc.Launcher;

/**
 * ClassLoaderUtil Offer static methods to add file or path to the system's
 * classpath.
 */
public class ClassLoaderUtil {
  /** Define CLASSES for class loading */
  private static Field CLASSES;
  /** Define LOG for outputting log information */
  private static final Log LOG = LogFactory.getLog(ClassLoaderUtil.class);
  /** Define ADDURL for method loading */
  private static Method ADDURL;
  static {
    try {
      CLASSES = ClassLoader.class.getDeclaredField("classes");
      ADDURL = URLClassLoader.class.getDeclaredMethod("addURL",
          new Class[] {URL.class});
    } catch (Exception e) {
      LOG.error("[ADDURL]", e);
      e.printStackTrace();
    }
    CLASSES.setAccessible(true);
    ADDURL.setAccessible(true);
  }

  /** The system class loader. */
  private static URLClassLoader SYSTEM =
      (URLClassLoader) getSystemClassLoader();
  /** The parent class loader */
  private static URLClassLoader EXT =
      (URLClassLoader) getExtClassLoader();

  /**
   * Returns the system class loader for delegation.  This is the default
   * delegation parent for new <tt>ClassLoader</tt> instances, and is
   * typically the class loader used to start the application.
   *
   * @return The system class loader for delegation.
   */
  public static ClassLoader getSystemClassLoader() {
    return ClassLoader.getSystemClassLoader();
  }

  /**
   * Returns the parent class loader for delegation. Some implementations may
   * use <tt>null</tt> to represent the bootstrap class loader. This method
   * will return <tt>null</tt> in such implementations if this class loader's
   * parent is the bootstrap class loader.
   *
   * @return The parent class loader for delegation.
   */
  public static ClassLoader getExtClassLoader() {
    return getSystemClassLoader().getParent();
  }

  /**
   * @return the value of the system class loader.
   */
  @SuppressWarnings("unchecked")
  public static List getClassesLoadedBySystemClassLoader() {
    return getClassesLoadedByClassLoader(getSystemClassLoader());
  }

  /**
   * @return the value of the parent class loader.
   */
  @SuppressWarnings("unchecked")
  public static List getClassesLoadedByExtClassLoader() {
    return getClassesLoadedByClassLoader(getExtClassLoader());
  }

  /**
   * Returns the value of the field represented by this Field, on
   * the specified object. The value is automatically wrapped in an
   * object if it has a primitive type.
   *
   * @return the value of the represented field in object.
   */
  @SuppressWarnings("unchecked")
  public static List getClassesLoadedByClassLoader(ClassLoader cl) {
    try {
      return (List) CLASSES.get(cl);
    } catch (Exception e) {
      LOG.error("[getClassesLoadedByClassLoader]", e);
    }
    return null;
  }

  public static URL[] getBootstrapURLs() {
    return Launcher.getBootstrapClassPath().getURLs();
  }

  public static URL[] getSystemURLs() {
    return SYSTEM.getURLs();
  }

  public static URL[] getExtURLs() {
    return EXT.getURLs();
  }

  /**
   * Prints class path list.
   *
   * @param ps A PrintStream to another output stream.
   * @param classPath The path list of URLs for loading classes and resources.
   */
  private static void list(PrintStream ps, URL[] classPath) {
    for (int i = 0; i < classPath.length; i++) {
      ps.println(classPath[i]);
    }
  }

  /**
   * Bootstrap class path
   */
  public static void listBootstrapClassPath() {
    listBootstrapClassPath(System.out);
  }

  /**
   * Bootstrap class path
   *
   * @param ps A PrintStream
   */
  public static void listBootstrapClassPath(PrintStream ps) {
    ps.println("BootstrapClassPath:");
    list(ps, getBootstrapClassPath());
  }

  /**
   * System class path
   */
  public static void listSystemClassPath() {
    listSystemClassPath(System.out);
  }

  /**
   * System class path
   *
   * @param ps A PrintStream
   */
  public static void listSystemClassPath(PrintStream ps) {
    ps.println("SystemClassPath:");
    list(ps, getSystemClassPath());
  }

  /**
   * Ext class path
   */
  public static void listExtClassPath() {
    listExtClassPath(System.out);
  }

  /**
   * Ext class path
   *
   * @param ps A PrintStream
   */
  public static void listExtClassPath(PrintStream ps) {
    ps.println("ExtClassPath:");
    list(ps, getExtClassPath());
  }

  public static URL[] getBootstrapClassPath() {
    return getBootstrapURLs();
  }

  public static URL[] getSystemClassPath() {
    return getSystemURLs();
  }

  public static URL[] getExtClassPath() {
    return getExtURLs();
  }

  /**
   * Add system class loader
   *
   * @param url An url
   */
  public static void addURL2SystemClassLoader(URL url) {
    try {
      ADDURL.invoke(SYSTEM, new Object[] {url});
    } catch (Exception e) {
      LOG.error("[addURL2SystemClassLoader]", e);
      e.printStackTrace();
    }
  }

  /**
   * Add Ext class loader
   *
   * @param url An url
   */
  public static void addURL2ExtClassLoader(URL url) {
    try {
      ADDURL.invoke(EXT, new Object[] {url});
    } catch (Exception e) {
      LOG.error("[addURL2ExtClassLoader]", e);
    }
  }

  /**
   * Add class path
   *
   * @param path The class path
   */
  public static void addClassPath(String path) {
    addClassPath(new File(path));
  }

  /**
   * Add Ext class path
   *
   * @param path The class path
   */
  public static void addExtClassPath(String path) {
    addExtClassPath(new File(path));
  }

  /**
   * Add class path
   *
   * @param dirOrJar The class path
   */
  public static void addClassPath(File dirOrJar) {
    try {
      addURL2SystemClassLoader(dirOrJar.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("[addClassPath]", e);
    }
  }

  /**
   * Add Ext class path
   *
   * @param dirOrJar The class path
   */
  public static void addExtClassPath(File dirOrJar) {
    try {
      addURL2ExtClassLoader(dirOrJar.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("[addExtClassPath]", e);
    }
  }
}
