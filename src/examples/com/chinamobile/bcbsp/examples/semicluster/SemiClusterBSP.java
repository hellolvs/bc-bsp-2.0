
package com.chinamobile.bcbsp.examples.semicluster;

import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * SemiClusterBSP.java This is the user-defined arithmetic which implements
 * {@link BSP}.
 */

public class SemiClusterBSP extends BSP<SemiClusterMessage> {
  
//  public static final Log LOG = LogFactory.getLog(SemiClusterBSP.class);
  /** Maximum number of iterations. */
  public static final String ITERATIONS = "iterations";
  /** Default value for ITERATIONS. */
  public static final int ITERATIONS_DEFAULT = 100;
  /** Maximum number of semi-clusters. */
  public static final String MAX_CLUSTERS = "max.clusters";
  /** Default value for maximum number of semi-clusters. */
  public static final int MAX_CLUSTERS_DEFAULT = 2;
  /** Maximum number of vertices in a semi-cluster. */
  public static final String CLUSTER_CAPACITY = "cluster.capacity";
  /** Default value for cluster capacity. */
  public static final int CLUSTER_CAPACITY_DEFAULT = 2;
  /** Boundary edge score factor. */
  public static final String SCORE_FACTOR = "score.factor";
  /** Default value for Boundary Edge Score Factor. */
  public static final float SCORE_FACTOR_DEFAULT = 0.5f;
  /** Comparator to sort clusters in the list based on their score. */
  private static final ClusterScoreComparator scoreComparator = new ClusterScoreComparator();
  
  @Override
  public void compute(Iterator<SemiClusterMessage> messages,
      BSPStaffContextInterface context) throws Exception {
    
    int iterations = ITERATIONS_DEFAULT;
    int maxClusters = MAX_CLUSTERS_DEFAULT;
    int clusterCapacity = CLUSTER_CAPACITY_DEFAULT;
    double scoreFactor = SCORE_FACTOR_DEFAULT;
    SemiClusterVertexLite vertex = (SemiClusterVertexLite) context.getVertex();
    if (context.getCurrentSuperStepCounter() == 0) {
      SemiCluster myCluster = new SemiCluster();
      myCluster.addVertex(vertex, scoreFactor);
      SemiClusterTreeSetWritable clusterList = new SemiClusterTreeSetWritable();
      clusterList.add(myCluster);
      
      vertex.setVertexValue(clusterList);
      sendMessageToAllEdges(context, clusterList);
      context.voltToHalt();
      return;
    }
    
    // For every cluster list received from neighbors and for every cluster in
    // the list, add current vertex if it doesn't already exist in the cluster
    // and the cluster is not full.
    //
    // Sort clusters received and newly formed clusters and send the top to all
    // neighbors
    //
    // Furthermore, update this vertex's list with received and newly formed
    // clusters the contain this vertex, sort them, and keep the top ones.
    SemiClusterTreeSetWritable unionedClusterSet = new SemiClusterTreeSetWritable();
    while (messages.hasNext()) {
      SemiClusterTreeSetWritable clusterSet = ((SemiClusterMessage) (messages
          .next())).getContent();
      unionedClusterSet.addAll(clusterSet);
      for (SemiCluster cluster : clusterSet) {
        boolean contains = cluster.getVertices().contains(
            new LongWritable(vertex.getVertexID()));
        if (!contains && cluster.getVertices().size() < clusterCapacity) {
          SemiCluster newCluster = new SemiCluster(cluster);
          newCluster.addVertex(vertex, scoreFactor);
          unionedClusterSet.add(newCluster);
          vertex.getVertexValue().add(newCluster);
        } else if (contains) {
          vertex.getVertexValue().add(cluster);
        }
      }
    }
    // If we have more than a maximum number of clusters, then we remove the
    // ones with the lowest score.
    Iterator<SemiCluster> iterator = unionedClusterSet.iterator();
    while (unionedClusterSet.size() > maxClusters) {
      iterator.next();
      iterator.remove();
    }
    iterator = vertex.getVertexValue().iterator();
    while (vertex.getVertexValue().size() > maxClusters) {
      iterator.next();
      iterator.remove();
    }
    sendMessageToAllEdges(context, unionedClusterSet);
    context.voltToHalt();
  }
  
  /**
   * Send message to all neighbor vertices.
   * @param context
   * @param unionedClusterSet
   */
  private void sendMessageToAllEdges(BSPStaffContextInterface context,
      SemiClusterTreeSetWritable unionedClusterSet) {
    Iterator<SemiClusterEdgeLite> outgoingEdges = context.getVertex()
        .getAllEdges().iterator();
    SemiClusterEdgeLite edge;
    SemiClusterMessage msg;
    while (outgoingEdges.hasNext()) {
      edge = (SemiClusterEdgeLite) outgoingEdges.next();
      msg = new SemiClusterMessage();
      msg.setContent(unionedClusterSet);
      msg.setMessageId(edge.getVertexID());
      context.send(msg);
    }
  }
  
  @Override
  public void initBeforeSuperStep(SuperStepContextInterface arg0) {
  }
  
  /**
   * A set of semi-clusters that is writable. We use a TreeSet since we want the
   * ability to sort semi-clusters.
   */
  public static class SemiClusterTreeSetWritable extends TreeSet<SemiCluster>
      implements WritableComparable<SemiClusterTreeSetWritable> {
    
    public static final String SEMICLUSTER_SPLIT_FLAG = "*";
    
    /**
     * Default Constructor. We ensure that this list always sorts semi-clusters
     * according to the comparator.
     */
    public SemiClusterTreeSetWritable() {
      super(scoreComparator);
    }
    
    /**
     * CompareTo method. Two lists of semi clusters are the same when: (i) they
     * have the same number of clusters, (ii) all their clusters are the same,
     * i.e. contain the same vertices For each cluster, check if it exists in
     * the other list of semi clusters
     * @param other
     *        A list with clusters to be compared with the current list
     * @return return 0 if two lists are the same
     */
    public final int compareTo(final SemiClusterTreeSetWritable other) {
      if (this.size() < other.size()) {
        return -1;
      }
      if (this.size() > other.size()) {
        return 1;
      }
      Iterator<SemiCluster> iterator1 = this.iterator();
      Iterator<SemiCluster> iterator2 = other.iterator();
      while (iterator1.hasNext()) {
        if (iterator1.next().compareTo(iterator2.next()) != 0) {
          return -1;
        }
      }
      return 0;
    }
    
    /**
     * Implements the readFields method of the Writable interface.
     * @param input
     *        Input to be read.
     * @throws IOException
     *         for IO.
     */
    @Override
    public final void readFields(final DataInput input) throws IOException {
      int size = input.readInt();
      for (int i = 0; i < size; i++) {
        SemiCluster c = new SemiCluster();
        c.readFields(input);
        add(c);
      }
    }
    
    /**
     * Implements the write method of the Writable interface.
     * @param output
     *        Output to be written.
     * @throws IOException
     *         for IO.
     */
    @Override
    public final void write(final DataOutput output) throws IOException {
      output.writeInt(size());
      for (SemiCluster c : this) {
        c.write(output);
      }
    }
    
    public void fromString(String semiClusterData) throws Exception {
      StringTokenizer str = new StringTokenizer(semiClusterData,
          SEMICLUSTER_SPLIT_FLAG);
      while (str.hasMoreTokens()) {
        SemiCluster cluster = new SemiCluster();
        cluster.fromString(str.nextToken());
        this.add(cluster);
      }
      
    }
    
    public String intoString() {
      String buffer = "";
      for (SemiCluster c : this) {
        buffer = buffer + SEMICLUSTER_SPLIT_FLAG + c;
      }
      return buffer;
    }
    
    public boolean add(SemiCluster e) {
      return super.add(e);
    }
  }
}
