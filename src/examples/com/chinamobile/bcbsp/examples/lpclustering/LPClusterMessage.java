package com.chinamobile.bcbsp.examples.lpclustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.comm.IMessage;

public class LPClusterMessage implements IMessage<Integer, Integer, Integer>{
	/** Id of message */
	  private int messageId;
	  /** value of message */
	  private int value;

	  @Override
	  public void write(DataOutput out) throws IOException {
	    out.writeInt(messageId);
	    out.writeInt(value);
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    this.messageId = in.readInt();
	    this.value = in.readInt();
	  }

	  @Override
	  public int getDstPartition() {
	    return -1;
	  }

	  @Override
	  public void setDstPartition(int partitionID) {

	  }

	  @Override
	  public String getDstVertexID() {
	    return String.valueOf(messageId);
	  }

	  @Override
	  public Integer getMessageId() {
	    return messageId;
	  }

	  @Override
	  public void setMessageId(Integer id) {
	    messageId = id;
	  }

	  @Override
	  public Integer getTag() {
	    return null;
	  }

	  @Override
	  public void setTag(Integer tag) {
	  }

	  @Override
	  public int getTagLen() {
	    return 0;
	  }

	  @Override
	  public Integer getData() {
	    return null;
	  }

	  @Override
	  public Integer getContent() {
	    return value;
	  }

	  @Override
	  public int getContentLen() {
	    return 4;
	  }

	  @Override
	  public void setContent(Integer content) {
	    value = content;
	  }

	  @Override
	  public String intoString() {
		  String message = this.getDstVertexID() + Constants.SPLIT_FLAG + String.valueOf(this.value);
	    return message;
	  }

	  @Override
	  public void fromString(String msgData) {
	  }

	  @Override
	  public long size() {
	    return 0;
	  }

	  @Override
	  public boolean combineIntoContainer(
	      Map<String, IMessage<Integer, Integer, Integer>> container) {
	    IMessage tmp = container.get(messageId);
	    if (tmp != null) {
	    	int val;
	       if((Integer)tmp.getContent() > value){
	    	   val = value;
	       }else{
	    	   val = (Integer)tmp.getContent();
	       }
	      tmp.setContent(val);
	      return true;
	    } else {
	      container.put(String.valueOf(messageId), this);
	      return false;
	    }
	  }
}
