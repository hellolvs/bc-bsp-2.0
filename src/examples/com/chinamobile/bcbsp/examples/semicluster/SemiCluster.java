
package com.chinamobile.bcbsp.examples.semicluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;
import com.chinamobile.bcbsp.api.Vertex;

/**
 * This class represents a semi-cluster.
 */
public class SemiCluster implements WritableComparable<SemiCluster> {
  
//  public static final Log LOG = LogFactory.getLog(SemiCluster.class);
  /** List of vertices */
  private HashSet<LongWritable> vertices;
  /** Score of current semi cluster. */
  private double score = 0.0;
  
  public double getScore() {
    return score;
  }
  
  /** Inner Score. */
  private double innerScore = 0.0;
  /** Boundary Score. */
  private double boundaryScore = 0.0;
  
  /** Constructor: Create a new empty Cluster. */
  public SemiCluster() {
    setVertices(new HashSet<LongWritable>());
    score = 1d;
    innerScore = 0d;
    boundaryScore = 0d;
  }
  
  /**
   * Constructor: Initialize a new Cluster.
   * @param cluster
   *        cluster object to initialize the new object
   */
  public SemiCluster(final SemiCluster cluster) {
    setVertices(new HashSet<LongWritable>());
    getVertices().addAll(cluster.getVertices());
    score = cluster.score;
    innerScore = cluster.innerScore;
    boundaryScore = cluster.boundaryScore;
  }
  
  /**
   * Adds a vertex to the cluster. Every time a vertex is added we also update
   * the inner and boundary score. Because vertices are only added to a
   * semi-cluster, we can save the inner and boundary scores and update them
   * incrementally. Otherwise, in order to re-compute it from scratch we would
   * need every vertex to send a friends-of-friends list, which is very
   * expensive.
   * @param vertex
   *        The new vertex to be added into the cluster
   * @param scoreFactor
   *        Boundary Edge Score Factor
   */
  public final void addVertex(SemiClusterVertexLite vertex,
      final double scoreFactor) {
    int vertexId = vertex.getVertexID();
    
    if (getVertices().add(new LongWritable(vertexId))) {
      if (size() == 1) {
        for (SemiClusterEdgeLite edge : vertex.getAllEdges()) {
          boundaryScore += edge.getEdgeValue();
        }
        score = 0.0;
      } else {
        for (SemiClusterEdgeLite edge : vertex.getAllEdges()) {
          if (getVertices().contains(new LongWritable(edge.getVertexID()))) {
            innerScore += edge.getEdgeValue();
            boundaryScore -= edge.getEdgeValue();
          } else {
            boundaryScore += edge.getEdgeValue();
          }
        }
        score = (innerScore - scoreFactor * boundaryScore)
            / (size() * (size() - 1) / 2);
      }
    }
  }
  
  /**
   * Returns size of semi cluster list.
   * @return Number of semi clusters in the list
   */
  public final int size() {
    return getVertices().size();
  }
  
  /**
   * Two semi clusters are the same when: (i) they have the same number of
   * vertices, (ii) all their vertices are the same
   * @param other
   *        Cluster to be compared with current cluster
   * @return 0 if two clusters are the same
   */
  @Override
  public final int compareTo(final SemiCluster other) {
    if (other == null) {
      return 1;
    }
    if (this.size() < other.size()) {
      return -1;
    }
    if (this.size() > other.size()) {
      return 1;
    }
    if (other.getVertices().containsAll(getVertices())) {
      return 0;
    }
    return -1;
  }
  
  /**
   * hashCode method.
   * @return result HashCode result
   */
  @Override
  public final int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result;
    if (getVertices() != null) {
      result += getVertices().hashCode();
    }
    return result;
  }
  
  /**
   * Equals method.
   * @param obj
   *        Object to compare if it is equal with.
   * @return boolean result.
   */
  @Override
  public final boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SemiCluster other = (SemiCluster) obj;
    if (getVertices() == null) {
      if (other.getVertices() != null) {
        return false;
      }
    } else if (!getVertices().equals(other.getVertices())) {
      return false;
    }
    return true;
  }
  
  /**
   * Convert object to string object.
   * @return string object
   */
  @Override
  public final String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("[ ");
    for (LongWritable v : this.getVertices()) {
      builder.append(v.toString());
      builder.append(" ");
    }
    builder.append(" | " + score + ", " + innerScore + ", " + boundaryScore
        + " ]");
    return builder.toString();
  }
  
  /**
   * Read fields.
   * @param input
   *        Input to be read.
   * @throws IOException
   *         for IO.
   */
  @Override
  public final void readFields(final DataInput input) throws IOException {
    setVertices(new HashSet<LongWritable>());
    int size = input.readInt();
    for (int i = 0; i < size; i++) {
      LongWritable e = new LongWritable();
      e.readFields(input);
      getVertices().add(e);
    }
    score = input.readDouble();
    innerScore = input.readDouble();
    boundaryScore = input.readDouble();
  }
  
  /**
   * Write fields.
   * @param output
   *        Output to be written.
   * @throws IOException
   *         for IO.
   */
  @Override
  public final void write(final DataOutput output) throws IOException {
    output.writeInt(size());
    for (LongWritable vertex : getVertices()) {
      vertex.write(output);
    }
    output.writeDouble(score);
    output.writeDouble(innerScore);
    output.writeDouble(boundaryScore);
  }
  
  public HashSet<LongWritable> getVertices() {
    return vertices;
  }
  
  public void setVertices(HashSet<LongWritable> vertices) {
    this.vertices = vertices;
  }
  
  public void fromString(String clusterData) throws Exception {
    clusterData = clusterData.replaceAll("\\[", "");
    clusterData = clusterData.replaceAll("\\]", "");
//    LOG.info("ljn test: the value data is " + clusterData);
    String[] buffer = new String[2];
    StringTokenizer str = new StringTokenizer(clusterData, "|");
    if (str.hasMoreElements()) {
      buffer[0] = str.nextToken();
    } else {
      throw new Exception();
    }
//    LOG.info("ljn test: the buffer 0 data is " +  buffer[0]);

    StringTokenizer str1 = new StringTokenizer(buffer[0], " ");
    HashSet<LongWritable> vertices = new HashSet<LongWritable>();
    
    while (str1.hasMoreTokens()) {
      String vertexString = str1.nextToken();
      long vertexId = 0L;
      if (vertexString.length() > 1) {
        char[] a = new char[2];
        a = vertexString.toCharArray();
        vertexId = a[1];
      } else {
        vertexId = Long.parseLong(vertexString);
      }
//      LOG.info("ljn test: the vertexId data is " +  vertexId);
      vertices.add(new LongWritable(vertexId));
    }
    
    if (str.hasMoreElements()) {
      buffer[1] = str.nextToken();
//      LOG.info("ljn test: the buffer[1] data is " +  buffer[1]);
      StringTokenizer str2 = new StringTokenizer(buffer[1], ",");
      if (str2.countTokens() < 3) {
        throw new RuntimeException();
      }
      double score = Double.parseDouble(str2.nextToken());
      double innerScore = Double.parseDouble(str2.nextToken());
      double boundaryScore = Double.parseDouble(str2.nextToken());
      
      this.setVertices(vertices);
      this.score = score;
      this.innerScore = innerScore;
      this.boundaryScore = boundaryScore;
    }
    
  }
}
