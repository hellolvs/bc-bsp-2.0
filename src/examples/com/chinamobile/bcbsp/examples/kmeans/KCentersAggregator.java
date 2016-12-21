/*
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

package com.chinamobile.bcbsp.examples.kmeans;

import com.chinamobile.bcbsp.api.Aggregator;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * KCentersAggregator
 */
public class KCentersAggregator extends Aggregator<KCentersAggregateValue> {
  /** Define LOG for outputting log information */
  public static final Log LOG = LogFactory.getLog(KCentersAggregator.class);

  @Override
  public KCentersAggregateValue aggregate(
      Iterable<KCentersAggregateValue> values) {
    KCentersAggregateValue kcenters = new KCentersAggregateValue();
    Iterator<KCentersAggregateValue> it = values.iterator();
    ArrayList<ArrayList<Float>> contents = null;
    // Init the contents with the first aggregate value.
    if (it.hasNext()) {
      contents = (ArrayList<ArrayList<Float>>) it.next().getValue().clone();
    }
    // Sum the same class's coordinate values and point's counts into the
    // content.
    while (it.hasNext()) {
      ArrayList<ArrayList<Float>> value = it.next().getValue();
      // Sum the corresponding element of the array except the first k rows.
      for (int i = 0; i < value.size(); i++) {
        ArrayList<Float> center = contents.get(i);
        ArrayList<Float> valueCenter = value.get(i);
        for (int j = 0; j < valueCenter.size(); j++) {
          center.set(j, center.get(j) + valueCenter.get(j));
        }
        contents.set(i, center);
      }
    }
    if (contents != null) {
      kcenters.setValue(contents);
    }
    return kcenters;
  }
}
