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

package com.chinamobile.bcbsp.examples.driver;

import com.chinamobile.bcbsp.examples.connectedcomponent.CCBSPDriverNew;
import com.chinamobile.bcbsp.examples.copra.Driver;
import com.chinamobile.bcbsp.examples.hits.HitsDriver;
import com.chinamobile.bcbsp.examples.kmeans.KMeansDriver;
import com.chinamobile.bcbsp.examples.lpclustering.LPClusterDriver;
import com.chinamobile.bcbsp.examples.pagerank.PageRankDriver;
import com.chinamobile.bcbsp.examples.semicluster.SemiClusterDriver;
import com.chinamobile.bcbsp.examples.simrank.SRDriver;
import com.chinamobile.bcbsp.examples.sssp.SSPDriver;
import com.chinamobile.bcbsp.examples.subgraph.SubGraphDriver;

import org.apache.hadoop.util.ProgramDriver;

/**
 * ExampleDriver An entry to drive examples of BC-BSP. Now, only implement three
 * examples. User can input "pagerank", "sssp" and "kmeans" to choose one
 * example to run.
 */
public class ExampleDriver {
  /**
   * Constructor.
   */
  private ExampleDriver() {
  }
  /**
   * main method.
   * @param argv command parameter
   * @throws Exception
   */
  public static void main(String[] argv) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("pagerank", PageRankDriver.class,
          "An example program that computes the pagerank value of web pages.");
      pgd.addClass("sssp", SSPDriver.class,
          "An example program that computes the single " +
            "source shortest path problem.");
      pgd.addClass("kmeans", KMeansDriver.class,
          "An example program that computes" +
          " the k-means algorithm.");
      pgd.addClass("hits",HitsDriver.class , "An example program that computes "+
    		  "the authority and hub value of nodes ");
      pgd.addClass("simrank", SRDriver.class, "An example program that computes "+
    		  "the similarity ");
      pgd.addClass("copra", Driver.class, "An example program that computes" +
      		" the commuty");
      pgd.addClass("cc", CCBSPDriverNew.class, "An example program that computes " +
      		"the connectedcomponent");
      pgd.addClass("lpcluster", LPClusterDriver.class, "An example program that computes " +
      		"the graph cluster");
      pgd.addClass("smc", SemiClusterDriver.class, "An example program that computes" +
      		" culster");
      pgd.addClass("subG", SubGraphDriver.class, "An example program that computes" +
      		" the subgraphmining");
      
      pgd.driver(argv);
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }
    System.exit(exitCode);
  }
}
