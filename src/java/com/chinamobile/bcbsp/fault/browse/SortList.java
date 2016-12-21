/**
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

package com.chinamobile.bcbsp.fault.browse;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * sort the given list according the value returned by the method; and the sort
 * determinate the correct order or reverted order
 * @param E
 *        list to sort
 */
public class SortList<E> {
  /**handle the log in this class*/
  private static final Log LOG = LogFactory.getLog(SortList.class);
  /**
   * sort the given list according the value returned by the method;
   * and the sort
   * determinate the correct order or reverted order
   * @param list
   *        list to sort.
   * @param method
   *        given method to get the value to compare.
   * @param sort
   *        decide the correct order or reverted order
   */
  public void Sort(List<E> list, final String method, final String sort) {
    Collections.sort(list, new Comparator<E>() {
      @SuppressWarnings("unchecked")
      public int compare(Object a, Object b) {
        int ret = 0;
        try {
          Object[] objs = null;
          Method m1 = ((E) a).getClass().getMethod(method, (Class<?>[]) objs);
          Method m2 = ((E) b).getClass().getMethod(method, (Class<?>[]) objs);
          if (sort != null && "desc".equals(sort)) {
            ret = m2.invoke(((E) b), objs).toString()
                .compareTo(m1.invoke(((E) a), objs).toString());
          } else {
            ret = m1.invoke(((E) a), objs).toString()
                .compareTo(m2.invoke(((E) b), objs).toString());
          }
        } catch (NoSuchMethodException ne) {
          LOG.error("[Sort]", ne);
          throw new RuntimeException("[Sort]", ne);
        } catch (IllegalAccessException ie) {
          throw new RuntimeException("[Sort]", ie);
        } catch (InvocationTargetException it) {
          throw new RuntimeException("[Sort]", it);
        }
        return ret;
      }
    });
  }
}
