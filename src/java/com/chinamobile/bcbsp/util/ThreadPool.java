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

/**
 * Thread Pool. The thread in this is used to send graph data.
 */
public class ThreadPool extends ThreadGroup {
  /** The default priority that is assigned to a thread. public to private */
  private static int DEFAULT_TOE_PRIORITY = Thread.NORM_PRIORITY - 1;
  /** the number of a thread */
  protected int nextSerialNumber = 0;
  protected int targetSize = 0;

  /**
   * Initialize the thread pool. There is threadNum threads in the pool.
   *
   * @param threadNum the number of a thread
   */
  public ThreadPool(int threadNum) {
    super("ToeThreads");
    for (int i = 0; i < threadNum; i++) {
      this.startNewThread();
    }
  }

  /**
   * Close the thread pool.
   */
  public void cleanup() {
    Thread[] toes = getAllThread();
    for (int i = 0; i < toes.length; i++) {
      if (!(toes[i] instanceof ThreadSignle)) {
        continue;
      }
      ThreadSignle t = (ThreadSignle) toes[i];
      while (t.isStatus()) {
      }
      killThread(t);
    }
  }

  /**
   * @return The number of ThreadSignle that are available.
   */
  public int getActiveToeCount() {
    Thread[] toes = getAllThread();
    int count = 0;
    for (int i = 0; i < toes.length; i++) {
      if ((toes[i] instanceof ThreadSignle) &&
          ((ThreadSignle) toes[i]).isAlive()) {
        count++;
      }
    }
    return count;
  }

  /**
   * @return The number of ThreadSignle. This may include killed Threads that
   *         were not replaced.
   */
  public int getToeCount() {
    Thread[] toes = getAllThread();
    int count = 0;
    for (int i = 0; i < toes.length; i++) {
      if (toes[i] instanceof ThreadSignle) {
        count++;
      }
    }
    return count;
  }

  /**
   * Obtain a free ThreadSignle to send data.
   *
   * @return a free ThreadSignle
   */
  public ThreadSignle getThread() {
    Thread[] toes = getAllThread();
    for (int i = 0; i < toes.length; i++) {
      if (!(toes[i] instanceof ThreadSignle)) {
        continue;
      }
      ThreadSignle toe = (ThreadSignle) toes[i];
      if (!toe.isStatus()) {
        return toe;
      }
    }
    return null;
  }

  /**
   * get all threads in pool.
   *
   * @return get all threads in pool
   */
  private Thread[] getAllThread() {
    Thread[] toes = new Thread[activeCount()];
    this.enumerate(toes);
    return toes;
  }

  /**
   * Create a new threadSignle
   */
  private synchronized void startNewThread() {
    ThreadSignle newThread = new ThreadSignle(this, this.nextSerialNumber++);
    newThread.setPriority(DEFAULT_TOE_PRIORITY);
    newThread.start();
  }

  /**
   * Kills specified thread.
   *
   * @param t a threadSignle
   */
  public void killThread(ThreadSignle t) {
    t.kill();
  }
}
