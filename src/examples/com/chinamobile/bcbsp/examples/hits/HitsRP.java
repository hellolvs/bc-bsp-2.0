package com.chinamobile.bcbsp.examples.hits;

import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.RecordParse;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.util.BSPJob;

public class HitsRP extends RecordParse{
	/**The log of the class.*/
	  private static final Log LOG = LogFactory.getLog(HitsRP.class);
	  /**The vertex to be Parse.*/
	  private Class<? extends Vertex<?, ?, ?>> vertexClass;
	  /**
	   * @param key The key of the vertex
	   * @param value The value of the vertex
	   * @return The vertex of the key and value
	   */
	  public Vertex recordParse(String key, String value) {
	    Vertex vertex = null;
	    //LOG.info("feng test : recordParse start.   " + "the key is " + key + " the value is " + value);
	    try {
	      vertex = vertexClass.newInstance();
	      vertex.fromString(key + Constants.KV_SPLIT_FLAG + value);
	    } catch (Exception e) {
	      LOG.error("RecordParse", e);
		    //LOG.info("feng test : recordParse return null. ");
	      return null;
	    }
	    //LOG.info("feng test : recordParse stop. ");
	    return vertex;
	  }
	  /**
	   * This method is used to parse a record and obtain VertexID .
	   * @param key The key of the vertex record
	   * @return the vertex id
	   */
	  public Text getVertexID(Text key) {
	    try {
	      StringTokenizer str = new StringTokenizer(key.toString(),
	          ":");
	      if (str.countTokens() != 3) {
	        return null;
	      }
		   // LOG.info("feng test : getVertexID is  " + key);
	      return new Text(str.nextToken());
	    } catch (Exception e) {
	      return null;
	    }
	  }
	  /**
	   * This method is used to initialize the RecordParseDefault.
	   * @param job The Bsp job.
	   */
	  public void init(BSPJob job) {
	    this.vertexClass = job.getVertexClass();
	    //LOG.info("feng test : HitsRP init  class is  " + vertexClass.getName());
	  }

}
