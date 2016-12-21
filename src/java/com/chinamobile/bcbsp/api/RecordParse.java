/**
 * CopyRight by Chinamobile
 *
 * RecordParse.java
 */

package com.chinamobile.bcbsp.api;


import com.chinamobile.bcbsp.util.BSPJob;

import org.apache.hadoop.io.Text;

/**
 * This class is used to parse a record as a HeadNode object. *
 */
public abstract class RecordParse {

  /**
   * This method is used to initialize the RecordParse extended.
   *
   * @param job
   *        BSPJob
   */
  public abstract void init(BSPJob job);

  /**
   * This method is used to parse a record as a Vertex object.
   *
   * @param key
   *        recordReader
   * @param value
   *        headNode
   * @return
   *        parse a record as a Vertex object
   */
  @SuppressWarnings("unchecked")
  public abstract Vertex recordParse(String key, String value);

  /**
   * This method is used to parse a record and obtain VertexID .
   *
   * @param key
   *        recordReader
   * @return
   *        VertexID
   */
  public abstract Text getVertexID(Text key);
}
